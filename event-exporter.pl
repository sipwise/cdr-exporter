#!/usr/bin/perl -w
use strict;
use v5.14;

use DBI;
use File::Temp;
use File::Copy;
use NGCP::CDR::Export;
use NGCP::CDR::Transfer;

our $DBHOST;
our $DBUSER;
our $DBPASS;
our $DBDB;

our $MAX_ROWS_PER_FILE;
our $EDRDIR;

our $FILTER_FLAPPING = 0;

our $PREFIX = 'sipwise';
our $VERSION = '001';
our $SUFFIX = 'edr';
our $FILES_OWNER = 'cdrexport';
our $FILES_GROUP = 'cdrexport';
our $FILES_MASK = '022';

our $TRANSFER_TYPE = "none";
our $TRANSFER_HOST;
our $TRANSFER_PORT = 22;
our $TRANSFER_USER = "cdrexport";
our $TRANSFER_PASS;
our $TRANSFER_REMOTE = "/home/jail/home/cdrexport";

our $EXPORT_FIELDS;
our $EXPORT_JOINS;
our $EXPORT_CONDITIONS;

my $collid = "eventexporter";
my $debug = 0;


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

open my $CONFIG, "$config_file" or die "Couldn't open the configuration file '$config_file'.\n";

while (<$CONFIG>) {
    chomp;                  # no newline
    s/^\s+//;               # no leading white
    s/^#.*//;                # no comments
    s/\s+$//;               # no trailing white
    next unless length;     # anything left?
    my ($var, $value) = split(/\s*=\s*/, $_, 2);
        no strict 'refs';
        $$var = $value;
}
close $CONFIG;

die "Invalid destination directory '$EDRDIR'\n"
    unless(-d $EDRDIR);


my @fields = ();
foreach my $f(split('\'\s*,\s*\#?\s*\'', $EXPORT_FIELDS)) {
    $f =~ s/^\'//; $f =~ s/\'$//;
    push @fields, $f;
}

my @joins = ();
foreach my $f(split('\}\s*,\s*{', $EXPORT_JOINS)) {
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
foreach my $f(split('\}\s*,\s*{', $EXPORT_CONDITIONS)) {
    $f =~ s/^\s*\{?\s*//; $f =~ s/\}\s*\}\s*$/}/;
    my ($a, $b) = split('\s*=>\s*{\s*', $f);
    $a =~ s/^\s*\'//; $a =~ s/\'$//g;
    $b =~ s/\s*\}\s*$//;

    my ($c, $d) = split('\s*=>\s*', $b);
    $c =~ s/^\s*\'//g; $c =~ s/\'\s*//;
    $d =~ s/^\s*\'//g; $d =~ s/\'\s*//;
    push @conditions, { $a => { $c => $d } };
}

my $dbh = DBI->connect('DBI:mysql:'.$DBDB, $DBUSER, $DBPASS)
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

    if($FILTER_FLAPPING) {
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
my $max = $MAX_ROWS_PER_FILE // $rec_idx;
do {
    my $recs = ($rec_idx > $max) ? $max : $rec_idx;

    $file_idx++;
    my @filevals = @vals[0 .. $recs-1];
    @vals = @vals[$recs .. @vals-1];
    NGCP::CDR::Export::write_file(
        \@filevals, $tempdir, $PREFIX, $VERSION, $file_ts, $file_idx, $SUFFIX,
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
    my $dst = "$EDRDIR/$file";
    if(-f $src) {
        DEBUG "### moving $src to $dst\n";
        copy($src, $dst);
        NGCP::CDR::Export::chownmod($dst, $FILES_OWNER, $FILES_GROUP, 0666, $FILES_MASK);
        if($TRANSFER_TYPE eq "sftp") {
            NGCP::CDR::Transfer::sftp(
                $dst, $TRANSFER_HOST, $TRANSFER_PORT, 
                $TRANSFER_REMOTE, $TRANSFER_USER, $TRANSFER_PASS,
            );
        }


    }
}
close($fh);


# vim: set tabstop=4 expandtab:
