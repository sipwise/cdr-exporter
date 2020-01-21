#!/usr/bin/perl

use strict;
use warnings;
use v5.14;
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::CDR::Exporter;

die("$0 already running") unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet
exit if scalar find_processes(qr/ngcp-cleanup-acc/);

my $stream_limit = 300000;
my @trailer = (
    { 'order by' => 'base_table.id' },
);

my @ignored_ids;
my @ids;

foreach my $stream (NGCP::CDR::Exporter::import_config('cdr-exporter.conf')) {
    #next if $stream eq 'default';
    next unless (confval("ENABLED") // 'no') eq 'yes';
    NGCP::CDR::Exporter::prepare_config('exporter', $stream);
    NGCP::CDR::Exporter::DEBUG("+++ Start cdr export stream '$stream' with DB " .
        (confval('DBUSER') || "(undef)") .
        "\@".confval('DBDB')." to ".confval('DESTDIR')."\n");
    # add fields we definitely need, will be removed during processing
    unshift @NGCP::CDR::Exporter::admin_fields, qw/
        base_table.id
        base_table.source_user_id
        base_table.destination_user_id
        base_table.source_provider_id
        base_table.destination_provider_id
    /;
    @ignored_ids = ();
    @ids = ();

    my $last_cdr_id = 0;
    my $limit = $stream_limit;
    NGCP::CDR::Exporter::build_query([ @trailer, { 'limit' => $limit }, ] , 'accounting.cdr', sub {
        my ($dbh,$joins,$conds) = @_;
        # for the default stream, we keep expecting the export_status condition defined in config.yml,
        # but custom streams need to be registered and the export_status cond is added implicitly:
        if ('default' ne $stream) {
            my $stmt = "insert into accounting.cdr_export_status (id,type) values (null,?)" .
                " on duplicate key update id = last_insert_id(id)";
            $dbh->do($stmt, undef, $stream) or die "Failed to register stream '$stream'";
            my $export_status_id = $dbh->{'mysql_insertid'};

            $stmt = "select coalesce(max(cdr_id),0) from accounting.cdr_export_status_data" .
                " where status_id = ?";
            my $sth = $dbh->prepare($stmt);
            $sth->execute($export_status_id) or die "Failed to obtain last processed cdr id of stream '$stream'";
            ($last_cdr_id) = $sth->fetchrow_array();
            $sth->finish();

            push @$joins, "left join accounting.cdr_export_status_data as __cesd" .
                " on __cesd.cdr_id = base_table.id and __cesd.status_id = " . $export_status_id;
            push @$conds, "base_table.id <= $last_cdr_id";
            push @$conds, "__cesd.export_status = 'unexported'";
        }
    });
    NGCP::CDR::Exporter::load_preferences();
    NGCP::CDR::Exporter::prepare_output();

    $limit = $limit - NGCP::CDR::Exporter::run(\&callback);
    if ('default' ne $stream and $limit > 0) {
        NGCP::CDR::Exporter::build_query([ @trailer, { 'limit' => $limit }, ], 'accounting.cdr', sub {
            my ($dbh,$joins,$conds) = @_;
            push @$conds, "base_table.id > $last_cdr_id";
        });
        NGCP::CDR::Exporter::run(\&callback);
    }

    #DEBUG "ignoring cdr ids " . (join "$sep", @ignored_ids);

    NGCP::CDR::Exporter::finish();

    if ('default' eq $stream) {
        update_export_status("accounting.cdr", \@ids, "ok");
        # TODO: should be tagged as ignored/skipped/whatever
        update_export_status("accounting.cdr", \@ignored_ids, "ok");
    } else {
        upsert_export_status("accounting.cdr", "accounting.cdr_export_status_data", \@ids, "ok");
        # TODO: should be tagged as ignored/skipped/whatever
        upsert_export_status("accounting.cdr", "accounting.cdr_export_status_data", \@ignored_ids, "ok");
    }

    NGCP::CDR::Exporter::commit();
}

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
    #my $quotes = confval('QUOTES');
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

    my $sep = prefval('system','cdr_export_field_separator') // confval('CSV_SEP');
    my $quotes = confval('QUOTES');
    my $escape_symbol = confval('CSV_ESC');
    my $line = join($sep, map { quote_field($_,$sep,$quotes,$escape_symbol); }
        apply_sclidui_rwrs('system',\@fields,scalar @fields - scalar @$row));
    write_reseller('system', $line, \&filestats_callback, $data_row);
    push(@ids, $id);

    if($src_uuid ne "0") {
        $sep = prefval($src_provid,'cdr_export_field_separator') // confval('CSV_SEP');
        $quotes = confval('QUOTES');
        $escape_symbol = confval('CSV_ESC');
        $line = join($sep, map { quote_field($_,$sep,$quotes,$escape_symbol); } apply_sclidui_rwrs($src_provid,$res_row));
        write_reseller_id($src_provid, $line, \&filestats_callback, $data_row);
    }
    if($dst_uuid ne "0") {
        if(confval('EXPORT_INCOMING') eq "no" && $src_provid ne $dst_provid) {
            # don't store incoming call to this reseller
        } else {
            if ($src_uuid ne '0' && $src_provid eq $dst_provid) {
                # skip duplicate entries
            } else {
                $sep = prefval($dst_provid,'cdr_export_field_separator') // confval('CSV_SEP');
                $quotes = confval('QUOTES');
                $escape_symbol = confval('CSV_ESC');
                $line = join($sep, map { quote_field($_,$sep,$quotes,$escape_symbol); } apply_sclidui_rwrs($dst_provid,$res_row));
                write_reseller_id($dst_provid, $line, \&filestats_callback, $data_row);
            }
        }
    }
}

__DATA__
This exists to allow the locking code at the beginning of the file to work.
DO NOT REMOVE THESE LINES!
