#!/usr/bin/perl

use strict;
use warnings;
use v5.14;
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::CDR::Exporter;

# $NGCP::CDR::Exporter::debug = 1;
# my $collid = "exporter";

die("$0 already running") unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

NGCP::CDR::Exporter::get_config('exporter', 'cdr-exporter.conf');

NGCP::CDR::Exporter::DEBUG("+++ Start run with DB " . (confval('DBUSER') || "(undef)") .
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
    { 'limit' => '300000' },
);

# working vars at beginning:
my @ignored_ids = ();
my @ids = ();

NGCP::CDR::Exporter::prepare_dbh(\@trailer, 'accounting.cdr');
NGCP::CDR::Exporter::load_preferences();
NGCP::CDR::Exporter::prepare_output();

NGCP::CDR::Exporter::run(\&callback);

sub filestats_callback {
    my ($data_row, $ref) = @_;

    my $out = $$ref || [0, 0, 0, 0, 0];
    for my $i (0 .. 4) { $$data_row[$i] //= 0 }

    ($$data_row[0] lt $$out[0] || !$$out[0]) and $$out[0] = $$data_row[0]; # min call start
    $$out[1] += $$data_row[1]; # sum duration
    ($$data_row[0] gt $$out[2] || !$$out[2]) and $$out[2] = $$data_row[0]; # max call start
    $$out[3] += $$data_row[2]; # sum carrier cost
    $$out[4] += $$data_row[3]; # sum customer cost

    $$ref = $out;
}

sub callback {
    my ($row, $res_row, $data_row) = @_;
    my $quotes = confval('QUOTES');
    #my $sep = prefval() || confval('CSV_SEP');
    my @fields = @{ $row };
    my $id = shift @fields;
    my $src_uuid = shift @fields;
    my $dst_uuid = shift @fields;
    my $src_provid = shift @fields;
    my $dst_provid = shift @fields;
    #@fields = map { quote_field($_); } (@fields);

    if(confval('EXPORT_INCOMING') eq "no" && $src_uuid eq "0") {
        push @ignored_ids, $id;
        return;
    }

    my $sep = prefval('system','cdr_export_field_separator') || confval('CSV_SEP');
    my $quotes = confval('QUOTES');
    my $escape_symbol = confval('CSV_ESC');
    my $line = join($sep, map { quote_field($_,$sep,$quotes,$escape_symbol); } @fields);
    write_reseller('system', $line, \&filestats_callback, $data_row);
    push(@ids, $id);

    if($src_uuid ne "0") {
        $sep = prefval('XXX','cdr_export_field_separator') || confval('CSV_SEP');
        $quotes = confval('QUOTES');
        $escape_symbol = confval('CSV_ESC');
        $line = join($sep, map { quote_field($_,$sep,$quotes,$escape_symbol); } @$res_row);
        write_reseller_id($src_provid, $line, \&filestats_callback, $data_row);
    }
    if($dst_uuid ne "0") {
        if(confval('EXPORT_INCOMING') eq "no" && $src_provid ne $dst_provid) {
            # don't store incoming call to this reseller
        } else {
            if ($src_uuid ne '0' && $src_provid eq $dst_provid) {
                # skip duplicate entries
            } else {
                $sep = prefval('XXX','cdr_export_field_separator') || confval('CSV_SEP');
                $quotes = confval('QUOTES');
                $escape_symbol = confval('CSV_ESC');
                $line = join($sep, map { quote_field($_,$sep,$quotes,$escape_symbol); } @$res_row);
                write_reseller_id($dst_provid, $line, \&filestats_callback, $data_row);
            }
        }
    }
}

#DEBUG "ignoring cdr ids " . (join "$sep", @ignored_ids);

NGCP::CDR::Exporter::finish();

update_export_status("accounting.cdr", \@ids, "ok");
upsert_export_status(\@ids, "ok") if confval('WRITE_EXTENDED_EXPORT_STATUS');
# TODO: should be tagged as ignored/skipped/whatever
update_export_status("accounting.cdr", \@ignored_ids, "ok");
upsert_export_status(\@ignored_ids, "ok") if confval('WRITE_EXTENDED_EXPORT_STATUS');

NGCP::CDR::Exporter::commit();

__DATA__
This exists to allow the locking code at the beginning of the file to work.
DO NOT REMOVE THESE LINES!
