#!/usr/bin/perl

use strict;
use warnings;
use v5.14;
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::CDR::Exporter;

die("$0 already running") unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

# $NGCP::CDR::Exporter::debug = 1;
# my $collid = "eventexporter";

$NGCP::CDR::Export::reseller_id_col = 'id';

# default config values overrides
my $config = {
    'PREFIX' => 'sipwise',
    'VERSION' => '001',
    'SUFFIX' => 'edr',
};

NGCP::CDR::Exporter::get_config('eventexporter', 'event-exporter.conf', $config);




# make sure we always select id, subscriber_id, type, old and new;
# if you change it, make sure to adapt slice in the loop too!
unshift @NGCP::CDR::Exporter::admin_fields, (qw/
    accounting.events.id accounting.events.subscriber_id accounting.events.reseller_id
    accounting.events.type accounting.events.old_status accounting.events.new_status
/);
my @trailer = (
    { 'order by' => 'accounting.events.id' },
    { 'limit' => '3000' },
);

NGCP::CDR::Exporter::prepare_dbh(\@trailer, 'accounting.events');


NGCP::CDR::Exporter::prepare_output();


my %lines = ();
my %res_lines;
my %filter = ();
my @filter_ids = ();

NGCP::CDR::Exporter::run(\&callback);

sub callback {
    my ($row, $res_row) = @_;
    my $quotes = confval('QUOTES');
    my $sep = confval('CSV_SEP');
    my @head = @{ $row }[0 .. 5];
    my ($id, $sub_id, $res_id, $type, $old, $new) = @head;
    my @fields = map { quote_field($_); } (@{ $row }[6 .. @{ $row }-1]);
    my $line = join "$sep", @fields;
    my $reseller_line = join "$sep", map { quote_field($_); } (@$res_row);

    if(confval('FILTER_FLAPPING')) {
        if($type =~ /^start_(.+)$/) {
            my $t = $1;
            my $k = "$sub_id;$t;$new";
            unless(exists $filter{$k}) {
                $filter{$k} = [$id];
            } else {
                push @{ $filter{$k} }, $id;
            }
            $lines{$id} = $line;
            $res_id and $res_lines{$res_id}{$id} = $reseller_line;
        } elsif(confval('MERGE_UPDATE') && $type =~ /^update_(.+)$/) {
            my $t = $1;
            my $k = "$sub_id;$t;$old";
            my $ids = $filter{$k} // [];
            if(@{ $ids }) {
                my $old_id = pop @{ $ids };
                ilog('debug', "... id $id is an update event of id $old_id, merge");
                delete $lines{$old_id};
                $res_id and delete $res_lines{$res_id}{$old_id};
                push @filter_ids, $old_id;
                $line =~ s/\"update_/\"start_/;
                $reseller_line =~ s/\"update_/\"start_/;
                $lines{$id} = $line;
                $res_id and $res_lines{$res_id}{$id} = $reseller_line;
                delete $filter{$k};
                $k = "$sub_id;$t;$new";
                push @{ $ids }, ($old_id, $id);
                $filter{$k} = $ids;
            } else {
                $lines{$id} = $line;
                $res_id and $res_lines{$res_id}{$id} = $reseller_line;
            }
        } elsif($type =~ /^(?:stop|end)_(.+)$/) {
            my $t = $1;
            my $k = "$sub_id;$t;$old";
            my $ids = $filter{$k} // [];
            if(@{ $ids }) {
                my $old_id = pop @{ $ids };
                ilog('debug', "... id $id is an end event of id $old_id, filter");
                push @filter_ids, ($id, $old_id);
                delete $lines{$old_id};
                $res_id and delete $res_lines{$res_id}{$old_id};
                $filter{$k} = $ids;
            } else {
                $lines{$id} = $line;
                $res_id and $res_lines{$res_id}{$id} = $reseller_line;
            }
        } else {
            $lines{$id} = $line;
            $res_id and $res_lines{$res_id}{$id} = $reseller_line;
        }
    } else {
        $lines{$id} = $line;
        $res_id and $res_lines{$res_id}{$id} = $reseller_line;
    }

}

my @vals = map { $lines{$_} } sort { int($a) <=> int($b) } keys %lines;
for my $val (@vals) {
    write_reseller('system', $val);
}
for my $res (keys(%res_lines)) {
    my $res_lines = $res_lines{$res};
    my @ids = keys(%$res_lines);
    @ids = sort {$a <=> $b} (@ids);
    for my $id (@ids) {
        my $val = $res_lines->{$id};
        $val or next;
        write_reseller_id($res, $val);
    }
}

NGCP::CDR::Exporter::finish();

my @ids = keys %lines;

update_export_status("accounting.events", \@filter_ids, "filtered");
update_export_status("accounting.events", \@ids, "ok");

NGCP::CDR::Exporter::commit();

__DATA__
This exists to allow the locking code at the beginning of the file to work.
DO NOT REMOVE THESE LINES!
