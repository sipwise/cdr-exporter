#!/usr/bin/perl -w
use strict;
use v5.14;

use Config::Simple;
use DBI;
use File::Temp;
use File::Copy;
use NGCP::CDR::Export;
use NGCP::CDR::Transfer;
use Data::Dumper;

my $collid = "eventexporter";
my $debug = 0;
# default config values
my $config = {
    'default.FILTER_FLAPPING' => 0,
    'default.MERGE_UPDATE' => 0,
    'default.PREFIX' => 'sipwise',
    'default.VERSION' => '001',
    'default.SUFFIX' => 'edr',
    'default.FILES_OWNER' => 'cdrexport',
    'default.FILES_GROUP' => 'cdrexport',
    'default.FILES_MASK' => '022',
    'default.TRANSFER_TYPE' => "none",
    'default.TRANSFER_PORT' => 22,
    'default.TRANSFER_USER' => "cdrexport",
    'default.TRANSFER_KEY' => "/root/.ssh/id_rsa",
    'default.TRANSFER_REMOTE' => "/home/jail/home/cdrexport"
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

die "Invalid destination directory '".$config->{'default.EDRDIR'}."'\n"
    unless(-d $config->{'default.EDRDIR'});


my @fields = ();
foreach my $f(@{$config->{'default.EXPORT_FIELDS'}}) {
    $f =~ s/^#.+//; next unless($f);
    $f =~ s/^\'//; $f =~ s/\'$//;
    push @fields, $f;
}

my @joins = ();
sub config2array {
    my $config_key = shift;
    return ('ARRAY' eq ref $config->{$config_key}) ? $config->{$config_key} : [$config->{$config_key}];
}
foreach my $f( @{config2array('default.EXPORT_JOINS')} ) {
    next unless($f);
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
foreach my $f(@{config2array('default.EXPORT_CONDITIONS')}) {
    next unless($f);
    $f =~ s/^\s*\{?\s*//; $f =~ s/\}\s*\}\s*$/}/;
    my ($a, $b) = split('\s*=>\s*{\s*', $f);
    $a =~ s/^\s*\'//; $a =~ s/\'$//g;
    $b =~ s/\s*\}\s*$//;

    my ($c, $d) = split('\s*=>\s*', $b);
    $c =~ s/^\s*\'//g; $c =~ s/\'\s*//;
    $d =~ s/^\s*\'//g; $d =~ s/\'\s*//;
    push @conditions, { $a => { $c => $d } };
}

my $dbh = DBI->connect('DBI:mysql:'.$config->{'default.DBDB'},
    $config->{'default.DBUSER'}, $config->{'default.DBPASS'})
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

    if($config->{'default.FILTER_FLAPPING'}) {
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
        } elsif($config->{'default.MERGE_UPDATE'} && $type =~ /^update_(.+)$/) {
            my $t = $1;
            my $k = "$sub_id;$t;$old";
            my $ids = $filter{$k} // [];
            if(@{ $ids }) {
                my $old_id = pop @{ $ids }; 
                say "... id $id is an update event of id $old_id, merge";
                delete $lines{$old_id};
                push @filter_ids, $old_id;
                my $line = join ",", @fields;
                $line =~ s/\"update_/\"start_/;
                $lines{$id} = $line;
                delete $filter{$k};
                $k = "$sub_id;$t;$new";
                push @{ $ids }, ($old_id, $id);
                $filter{$k} = $ids;
            } else {
                my $line = join ",", @fields;
                $lines{$id} = $line;
                $rec_idx++;
            }
        } elsif($type =~ /^(?:stop|end)_(.+)$/) {
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
my $max = $config->{'default.MAX_ROWS_PER_FILE'} // $rec_idx;
do {
    my $recs = ($rec_idx > $max) ? $max : $rec_idx;

    $file_idx++;
    my @filevals = @vals[0 .. $recs-1];
    @vals = @vals[$recs .. @vals-1];
    NGCP::CDR::Export::write_file(
        \@filevals, $tempdir, $config->{'default.PREFIX'},
        $config->{'default.VERSION'}, $file_ts, $file_idx, $config->{'default.SUFFIX'},
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
    my $dst = $config->{'default.EDRDIR'}."/$file";
    if(-f $src) {
        DEBUG "### moving $src to $dst\n";
        copy($src, $dst);
        NGCP::CDR::Export::chownmod($dst, $config->{'default.FILES_OWNER'},
            $config->{'default.FILES_GROUP'}, oct(666),
            $config->{'default.FILES_MASK'});
        if($config->{'default.TRANSFER_TYPE'} eq "sftp") {
            NGCP::CDR::Transfer::sftp(
                $dst, $config->{'default.TRANSFER_HOST'},
                $config->{'default.TRANSFER_PORT'},
                $config->{'default.TRANSFER_REMOTE'},
                $config->{'default.TRANSFER_USER'},
                $config->{'default.TRANSFER_PASS'},
            );
        } elsif($config->{'default.TRANSFER_TYPE'} eq "sftp-sh") {
            NGCP::CDR::Transfer::sftp_sh(
                $dst, $config->{'default.TRANSFER_HOST'},
                $config->{'default.TRANSFER_PORT'},
                $config->{'default.TRANSFER_REMOTE'},
                $config->{'default.TRANSFER_USER'},
                $config->{'default.TRANSFER_KEY'},
            );
        }
    }
}
close($fh);


# vim: set tabstop=4 expandtab:
