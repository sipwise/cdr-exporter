#!/usr/bin/perl -w
use strict;
use v5.14;

use Config::Simple;
use DBI;
use File::Temp;
use File::Copy;
use NGCP::CDR::Export;
use NGCP::CDR::Transfer;

my $collid = "eventexporter";
my $debug = 0;
# default config values
my $config = {
    FILTER_FLAPPING => 0,
    PREFIX => 'sipwise',
    VERSION => '001',
    SUFFIX => 'edr',
    FILES_OWNER => 'cdrexport',
    FILES_GROUP => 'cdrexport',
    FILES_MASK => '022',
    TRANSFER_TYPE => "none",
    TRANSFER_PORT => 22,
    TRANSFER_USER => "cdrexport",
    TRANSFER_REMOTE => "/home/jail/home/cdrexport"
};

sub DEBUG {
    say join (' ', @_);
}

my @config_paths = (qw#
    /etc/ngcp-cdr-exporter/
    .
#);


my $cf = 'event-exporter.conf';
my $config_file;
foreach my $cp(@config_paths) {
    if(-f "$cp/$cf") {
        $config_file = "$cp/$cf";
        last;
    }
}
die "Config file $cf not found in path " . (join " or ", @config_paths) . "\n"
    unless $config_file;

Config::Simple->import_from("$config_file" , \%{$config}) or
    die "Couldn't open the configuration file '$config_file'.\n";

die "Invalid destination directory '".$config->{EDRDIR}."'\n"
    unless(-d $config->{EDRDIR});


my @fields = ();
foreach my $f(split('\'\s*,\s*\#?\s*\'', $config->{EXPORT_FIELDS})) {
    $f =~ s/^\'//; $f =~ s/\'$//;
    push @fields, $f;
}

my @joins = ();
foreach my $f(split('\}\s*,\s*{', $config->{EXPORT_JOINS})) {
    $f =~ s/^\s*\{?\s*//; $f =~ s/\}\s*\}\s*$/}/;
    my ($a, $b) = split('\s*=>\s*{\s*', $f);
    $a =~ s/^\s*\'//; $a =~ s/\'$//g;
    $b =~ s/\s*\}\s*$//;

    my ($c, $d) = split('\s*=>\s*', $b);
    $c =~ s/^\s*\'//g; $c =~ s/\'\s*//;
    $d =~ s/^\s*\'//g; $d =~ s/\'\s*//;
    push @joins, { $a => { $c => $d } };
}

my @conditions = ();
foreach my $f(split('\}\s*,\s*{', $config->{EXPORT_CONDITIONS})) {
    $f =~ s/^\s*\{?\s*//; $f =~ s/\}\s*\}\s*$/}/;
    my ($a, $b) = split('\s*=>\s*{\s*', $f);
    $a =~ s/^\s*\'//; $a =~ s/\'$//g;
    $b =~ s/\s*\}\s*$//;

    my ($c, $d) = split('\s*=>\s*', $b);
    $c =~ s/^\s*\'//g; $c =~ s/\'\s*//;
    $d =~ s/^\s*\'//g; $d =~ s/\'\s*//;
    push @conditions, { $a => { $c => $d } };
}

my $dbh = DBI->connect('DBI:mysql:'.$config->{DBDB},
    $config->{DBUSER}, $config->{DBPASS})
    or die "failed to connect to db: $DBI::errstr";
$dbh->{mysql_auto_reconnect} = 1;
$dbh->{AutoCommit} = 0;


my @trailer = (
    { 'order by' => 'accounting.events.id' },
);

# make sure we always select id, subscriber_id, type, old and new;
# if you change it, make sure to adapt slice in the loop too!
unshift @fields, (qw/
    accounting.events.id accounting.events.subscriber_id accounting.events.type
    accounting.events.old_status accounting.events.new_status
/);

my @intjoins = ();
foreach my $f(@joins) {
    my ($table, $keys) = %{ $f };
    my ($foreign_key, $own_key) = %{ $keys };
    push @intjoins, "left outer join $table on $foreign_key = $own_key";
}
my @conds = ();
foreach my $f(@conditions) {
    my ($field, $match) = %{ $f };
    my ($op, $val) = %{ $match };
    push @conds, "$field $op $val";
}
my @trail = ();
foreach my $f(@trailer) {
    my ($key, $val) = %{ $f };
    push @trail, "$key $val";
}

my $file_ts = NGCP::CDR::Export::get_ts_for_filename;
my $mark = NGCP::CDR::Export::get_mark($dbh, $collid);

my $q = "select " . 
    join(", ", @fields) . " from accounting.events " . 
    join(" ", @intjoins) . " " .
    "where " . join(" and ", @conds) . " " .
    join(" ", @trail);

DEBUG $q if $debug;

my $tempfh = File::Temp->newdir(undef, CLEANUP => 1);
my $tempdir = $tempfh->dirname;

my $sth = $dbh->prepare($q);
$sth->execute();

my ($rec_idx, $file_idx) = (0, $mark->{lastseq});
my $written = 0;
my %lines = ();
my $rows = $sth->fetchall_arrayref();
my %filter = ();
my @filter_ids = ();
while(my $row = shift @{ $rows }) {
    my @head = @{ $row }[0 .. 4];
    my ($id, $sub_id, $type, $old, $new) = @head;
    my @fields = map { defined $_ ? "\"$_\"" : '""' } (@{ $row }[5 .. @{ $row }-1]);

    if($config->{FILTER_FLAPPING}) {
        if($type =~ /^start_(.+)$/) {
            my $t = $1;
            my $k = "$sub_id;$t;$new";
            unless(exists $filter{$k}) {
                $filter{$k} = [$id];
            } else {
                push @{ $filter{$k} }, $id;
            }
            my $line = join ",", @fields;
            $lines{$id} = $line;
            $rec_idx++;
        } elsif($type =~ /^end_(.+)$/) {
            my $t = $1;
            my $k = "$sub_id;$t;$old";
            my $ids = $filter{$k} // [];
            if(@{ $ids }) {
                my $old_id = pop @{ $ids }; 
                say "... id $id is an end event of id $old_id, filter";
                push @filter_ids, ($id, $old_id);
                delete $lines{$old_id};
                $rec_idx--;
                $filter{$k} = $ids;
            } else {
                my $line = join ",", @fields;
                $lines{$id} = $line;
                $rec_idx++;
            }
        } else {
            my $line = join ",", @fields;
            $lines{$id} = $line;
            $rec_idx++;
        }
    } else {
        my $line = join ",", @fields;
        $lines{$id} = $line;
        $rec_idx++;
    }

}

my @vals = map { $lines{$_} } sort { int($a) <=> int($b) } keys %lines;
my @ids = keys %lines;
my $max = $config->{MAX_ROWS_PER_FILE} // $rec_idx;
do {
    my $recs = ($rec_idx > $max) ? $max : $rec_idx;

    $file_idx++;
    my @filevals = @vals[0 .. $recs-1];
    @vals = @vals[$recs .. @vals-1];
    NGCP::CDR::Export::write_file(
        \@filevals, $tempdir, $config->{PREFIX},
        $config->{VERSION}, $file_ts, $file_idx, $config->{SUFFIX},
    );
    $rec_idx -= $recs;

} while($rec_idx > 0);

NGCP::CDR::Export::update_export_status($dbh, "accounting.events", \@filter_ids, "filtered");
NGCP::CDR::Export::update_export_status($dbh, "accounting.events", \@ids, "ok");
NGCP::CDR::Export::set_mark($dbh, $collid, { lastseq => $file_idx });

$dbh->commit or die("failed to commit db changes: " . $dbh->errstr);

opendir(my $fh, $tempdir);
foreach my $file(readdir($fh)) {
    my $src = "$tempdir/$file";
    my $dst = $config->{EDRDIR}."/$file";
    if(-f $src) {
        DEBUG "### moving $src to $dst\n";
        copy($src, $dst);
        NGCP::CDR::Export::chownmod($dst, $config->{FILES_OWNER},
            $config->{FILES_GROUP}, '0666', $config->{FILES_MASK});
        if($config->{TRANSFER_TYPE} eq "sftp") {
            NGCP::CDR::Transfer::sftp(
                $dst, $config->{TRANSFER_HOST}, $config->{TRANSFER_PORT},
                $config->{TRANSFER_REMOTE}, $config->{TRANSFER_USER},
                $config->{TRANSFER_PASS},
            );
        }


    }
}
close($fh);


# vim: set tabstop=4 expandtab:
