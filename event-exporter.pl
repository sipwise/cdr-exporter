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

my $dbh = DBI->connect('DBI:mysql:accounting', 'export', 'export')
    or die "failed to connect to db: $DBI::errstr";
$dbh->{mysql_auto_reconnect} = 1;
$dbh->{AutoCommit} = 0;


my @trailer = (
    { 'order by' => 'accounting.events.id' },
);

unless($fields[0] eq "accounting.events.id") {
    die "First field must always be 'accounting.events.id'\n";
}

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
my @lines = ();
my @ids = ();
my $rows = $sth->fetchall_arrayref();
while(my $row = shift @{ $rows }) {
    my @fields = map { defined $_ ? "\"$_\"" : '""' } @{ $row };
    my $line = join ",", @fields;
    push @lines, $line;

    $rec_idx++;

    if(($MAX_ROWS_PER_FILE && $rec_idx >= $MAX_ROWS_PER_FILE) || @{ $rows } == 0) {
        $rec_idx = 0;
        $file_idx++;

        NGCP::CDR::Export::write_file(
            \@lines, $tempdir, $PREFIX, $VERSION, $file_ts, $file_idx, $SUFFIX,
        );
        @lines = ();
    }
    push @ids, $row->[0];
}
# write empty file in case of no records
unless(@ids) {
    NGCP::CDR::Export::write_file(
        \@lines, $tempdir, $PREFIX, $VERSION, $file_ts, $file_idx, $SUFFIX,
    );
}

NGCP::CDR::Export::update_export_status($dbh, "accounting.events", \@ids);
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
