#!/usr/bin/perl

use strict;
use warnings;
use v5.14;

use NGCP::CDR::Exporter;

# $NGCP::CDR::Exporter::debug = 1;
# my $collid = "exporter";

NGCP::CDR::Exporter::get_config('exporter', 'cdr-exporter.conf');


print("+++ Start run with DB " . (confval('DBUSER') || "(undef)") .
	"\@".confval('DBDB')." to ".confval('PREFIX')."\n");

# add fields we definitely need, will be removed during processing
unshift @NGCP::CDR::Exporter::admin_fields, qw/
    accounting.cdr.id
    accounting.cdr.source_user_id
    accounting.cdr.destination_user_id
    accounting.cdr.source_provider_id
    accounting.cdr.destination_provider_id
/;

my @trailer = (
    { 'order by' => 'accounting.cdr.id' },
);

# working vars at beginning:
my @ignored_ids = ();
my @ids = ();

NGCP::CDR::Exporter::prepare_dbh(\@trailer, 'accounting.cdr');

NGCP::CDR::Exporter::prepare_output();

NGCP::CDR::Exporter::run(\&callback);

sub callback {
    my ($row, $res_row) = @_;
    my $quotes = NGCP::CDR::Exporter::confval('QUOTES');
    my @fields = @{ $row };
    my $id = shift @fields;
    my $src_uuid = shift @fields;
    my $dst_uuid = shift @fields;
    my $src_provid = shift @fields;
    my $dst_provid = shift @fields;
    @fields = map { defined $_ ? $quotes . $_ . $quotes : $quotes. $quotes } (@fields);

    if(confval('EXPORT_INCOMING') eq "no" && $src_uuid eq "0") {
        push @ignored_ids, $id;
        next;
    }

    my $line = join ",", @fields;
    write_reseller('system', $line);
    push(@ids, $id);

    my $reseller_line = join ",", map { defined $_ ? $quotes . $_ . $quotes : $quotes. $quotes } (@$res_row);

    if($src_uuid ne "0") {
	write_reseller_id($src_provid, $reseller_line);
    }
    if($dst_uuid ne "0") {
        if(confval('EXPORT_INCOMING') eq "no" && $src_provid ne $dst_provid) {
            # don't store incoming call to this reseller
        } else {
	    if ($src_uuid ne '0' && $src_provid eq $dst_provid) {
		# skip duplicate entries
	    } else {
		write_reseller_id($dst_provid, $reseller_line);
	    }
        }
    }
}

#DEBUG "ignoring cdr ids " . (join ",", @ignored_ids);

NGCP::CDR::Exporter::finish();

update_export_status("accounting.cdr", \@ids, "ok");
# TODO: should be tagged as ignored/skipped/whatever
update_export_status("accounting.cdr", \@ignored_ids, "ok");

NGCP::CDR::Exporter::commit();
