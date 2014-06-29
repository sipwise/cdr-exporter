#!/usr/bin/perl -w
use strict;
use v5.14;

use DBI;
use NGCP::CDR::Export;

my $debug = 1;

my $max_rec_idx = 5000;

sub DEBUG {
    say join (' ', @_);
}

my $dbh = DBI->connect('DBI:mysql:accounting', 'export', 'export')
    or die "failed to connect to db: $DBI::errstr";
$dbh->{mysql_auto_reconnect} = 1;

my @fields = (
    'accounting.events.id',
    'accounting.events.type',
    'billing.contracts.external_id',
    'billing.contacts.company',
    'billing.voip_subscribers.external_id',
    'concat(voip_numbers_tmp.cc, voip_numbers_tmp.ac, voip_numbers_tmp.sn)',
    #'accounting.events.old_status',
    'old_profile.name',
    #'accounting.events.new_status',
    'new_profile.name',
    'from_unixtime(accounting.events.timestamp)',
);
my @joins = (
    { 'provisioning.voip_subscribers' => { 'provisioning.voip_subscribers.id' => 'accounting.events.subscriber_id' } },
    { 'billing.voip_subscribers' => { 'billing.voip_subscribers.uuid' => 'provisioning.voip_subscribers.uuid' } },
    { 'billing.contracts' => { 'billing.contracts.id' => 'billing.voip_subscribers.contract_id' } },
    { 'billing.contacts' => { 'billing.contacts.id' => 'billing.contracts.contact_id' } },
    { '(select vn1.* from billing.voip_numbers vn1 left outer join billing.voip_numbers vn2 on vn1.subscriber_id = vn2.subscriber_id and vn1.id > vn2.id) as voip_numbers_tmp' => { 'billing.voip_subscribers.id' => 'voip_numbers_tmp.subscriber_id' } },
    { 'provisioning.voip_subscriber_profiles as old_profile' => { 'old_profile.id' => 'accounting.events.old_status' } },
    { 'provisioning.voip_subscriber_profiles as new_profile' => { 'new_profile.id' => 'accounting.events.new_status' } },

);
my @conditions = (
    { 'accounting.events.export_status' => { '=' => '"unexported"' } },
#    { 'accounting.events.timestamp' => { '>=' => 'unix_timestamp(date_sub(concat(curdate()," 00:00:00"), interval 1 day))' } },
#    { 'accounting.events.timestamp' => { '<' => 'unix_timestamp(concat(curdate()," 00:00:00"))' } },
);
my @trailer = (
    { 'order by' => 'accounting.events.id' },
);

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

my $q = "select " . 
    join(", ", @fields) . " from accounting.events " . 
    join(" ", @intjoins) . " " .
    "where " . join(" and ", @conds) . " " .
    join(" ", @trail);

DEBUG $q if $debug;

my $sth = $dbh->prepare($q);
$sth->execute();

# TODO: get file_idx from lastseq in db
my ($rec_idx, $file_idx) = (0, 0);
my @lines = ();
my $rows = $sth->fetchall_arrayref();
while(my $row = shift @{ $rows }) {
    my @fields = map { defined $_ ? "\"$_\"" : '""' } @{ $row };
    my $line = join ",", @fields;
    push @lines, $line;

    $rec_idx++;

    if($rec_idx >= $max_rec_idx || @{ $rows } == 0) {
        $rec_idx = 0;
        $file_idx++;

        NGCP::CDR::Export::write_file(
            \@lines, '/tmp', 'swpbx', 'v001', $file_ts, $file_idx, 'edr',
            'agranig', 'agranig', '022',
        );
        @lines = ();
    }
}

# vim: set tabstop=4 expandtab:
