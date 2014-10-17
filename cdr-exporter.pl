#!/usr/bin/perl

use strict;
use warnings;
use v5.14;

use Config::Simple;
use DBI;
use Digest::MD5;
use NGCP::CDR::Export;
use File::Temp;
use File::Copy;
use File::Path;

my $debug = 1;
my $collid = "exporter";

# default config values
my $config = {
    'default.PREFIX' => 'ngcp',
    'default.VERSION' => '007',
    'default.SUFFIX' => 'cdr',
    'default.FILES_OWNER' => 'cdrexport',
    'default.FILES_GROUP' => 'cdrexport',
    'default.FILES_MASK' => '022',
    'default.TRANSFER_TYPE' => "none",
    'default.TRANSFER_PORT' => 22,
    'default.TRANSFER_USER' => "cdrexport",
    'default.TRANSFER_REMOTE' => "/home/jail/home/cdrexport"
};

sub DEBUG {
    say join (' ', @_);
}

my @config_paths = (qw#
    /etc/ngcp-cdr-exporter/
    .
#);
my $cf = 'cdr-exporter.conf';
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

die "Invalid destination directory '".$config->{'default.CDRDIR'}."'\n"
    unless(-d $config->{'default.CDRDIR'});

my $now = time();
my @now = localtime($now);

my @admin_fields = ();
foreach my $f(@{$config->{'default.ADMIN_EXPORT_FIELDS'}}) {
    $f =~ s/^#.+//; next unless($f);
    $f =~ s/^\'//; $f =~ s/\'$//;
    push @admin_fields, $f;
}
my @reseller_fields = ();
foreach my $f(@{$config->{'default.RESELLER_EXPORT_FIELDS'}}) {
    $f =~ s/^#.+//; next unless($f);
    $f =~ s/^\'//; $f =~ s/\'$//;
    push @reseller_fields, $f;
}

my @joins = ();
foreach my $f(@{$config->{'default.EXPORT_JOINS'}}) {
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
foreach my $f(@{$config->{'default.EXPORT_CONDITIONS'}}) {
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

my @trailer = (
    { 'order by' => 'accounting.cdr.id' },
);

my $dbh = DBI->connect("dbi:mysql:" . $config->{'default.DBDB'} .
	";host=".$config->{'default.DBHOST'},
	$config->{'default.DBUSER'}, $config->{'default.DBPASS'})
    or die "failed to connect to db: $DBI::errstr";

$dbh->{mysql_auto_reconnect} = 1;
$dbh->{AutoCommit} = 0;

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

print("+++ Start run with DB " . ($config->{'default.DBUSER'} || "(undef)") .
	"\@".$config->{'default.DBDB'}." to ".$config->{'default.PREFIX'}."\n");

# extract positions of reseller fields from admin fields
my @reseller_positions = ();
my %reseller_index;
@reseller_index{@admin_fields} = (0..$#admin_fields);
for(my $i = 0; $i < @reseller_fields; $i++) {
    my $name = $reseller_fields[$i];
    unless(exists $reseller_index{$name}) {
        die "Invalid RESELLER_EXPORT_FIELDS element '$name', not available in ADMIN_EXPORT_FIELDS!";
    }
    push @reseller_positions, $reseller_index{$name};
}



# add fields we definitely need, will be removed during processing
unshift @admin_fields, qw/
    accounting.cdr.id
    accounting.cdr.source_user_id
    accounting.cdr.destination_user_id
    accounting.cdr.source_provider_id
    accounting.cdr.destination_provider_id
/;

my $q = "select " .
    join(", ", @admin_fields) . " from accounting.cdr " .
    join(" ", @intjoins) . " " .
    "where " . join(" and ", @conds) . " " .
    join(" ", @trail);

#DEBUG $q if $debug;

my $tempfh = File::Temp->newdir(undef, CLEANUP => 1);
my $tempdir = $tempfh->dirname;

my $sth = $dbh->prepare($q);
$sth->execute();

my $written = 0;
my @ignored_ids = ();

my $reseller_names = {};
my $reseller_ids = {};
my $reseller_lines = {};

while(my $row = $sth->fetchrow_arrayref) {
    # agranig: no quoting of fields
    # my @fields = map { defined $_ ? "\"$_\"" : '""' } (@{ $row });
    my @fields = @{ $row };
    my $id = shift @fields;
    my $src_uuid = shift @fields;
    my $dst_uuid = shift @fields;
    my $src_provid = shift @fields;
    my $dst_provid = shift @fields;
    @fields = map { defined $_ ? "'$_'" : "''" } (@fields);

    if($config->{'default.EXPORT_INCOMING'} eq "no" && $src_uuid eq "0") {
        push @ignored_ids, $id;
        next;
    }

    my $line = join ",", @fields;
    $reseller_lines->{'system'}->{$id} = $line;

    my @reseller_fields = @fields[@reseller_positions];
    my $reseller_line = join ",", @reseller_fields;

    if($src_uuid ne "0") {
        if(!exists $reseller_names->{$src_provid}) {
            $reseller_names->{$src_provid} = NGCP::CDR::Export::get_reseller_name($dbh, $src_provid);
            $reseller_ids->{$reseller_names->{$src_provid}} = $src_provid;
        }
        $reseller_lines->{$reseller_names->{$src_provid}}->{$id} = $reseller_line;
    }
    if($dst_uuid ne "0") {
        if($config->{'default.EXPORT_INCOMING'} eq "no" && $src_provid ne $dst_provid) {
            # don't store incoming call to this reseller
        } else {
            if(!exists $reseller_names->{$dst_provid}) {
                $reseller_names->{$dst_provid} = NGCP::CDR::Export::get_reseller_name($dbh, $dst_provid);
                $reseller_ids->{$reseller_names->{$dst_provid}} = $dst_provid;
            }
            $reseller_lines->{$reseller_names->{$dst_provid}}->{$id} = $reseller_line;
        }
    }
}

#DEBUG "ignoring cdr ids " . (join ",", @ignored_ids);

my $full_name = (defined $config->{'default.FULL_NAMES'} && $config->{'default.NAMES'} eq "yes" ? 1 : 0);
my $monthly_dir = (defined $config->{'default.MONTHLY_DIR'} && $config->{'default.MONTHLY_DIR'} eq "yes" ? 1 : 0); 
my $daily_dir = (defined $config->{'default.DAILY_DIR'} && $config->{'default.DAILY_DIR'} eq "yes" ? 1 : 0);
my $file_ts = NGCP::CDR::Export::get_ts_for_filename;
my $dname = "";
if($monthly_dir && !$daily_dir) {
    $dname .= sprintf("%04i%02i", $now[5] + 1900, $now[4] + 1);
    $full_name or $file_ts = sprintf("%02i%02i%02i%02i", @now[3,2,1,0]);
} elsif(!$monthly_dir && $daily_dir) {
    $dname .= sprintf("%04i%02i%02i", $now[5] + 1900, $now[4] + 1, $now[3]);
    $full_name or $file_ts = sprintf("%02i%02i%02i", @now[2,1,0]);
} elsif($monthly_dir && $daily_dir) {
    $dname .= sprintf("%04i%02i/%02i", $now[5] + 1900, $now[4] + 1, $now[3]);
    $full_name or $file_ts = sprintf("%02i%02i%02i", @now[2,1,0]);
} 


my @ids = keys %{ $reseller_lines->{'system'} };
my @resellers = keys $reseller_lines;
# make sure to process system user first:
@resellers = grep { $_ ne 'system' } @resellers;
unshift @resellers, 'system';

# we write empty cdrs for resellers which didn't have a call during this
# export run, so get them into the list
my $missing_resellers = NGCP::CDR::Export::get_missing_resellers($dbh, [ keys $reseller_names ]);
for(my $i = 0; $i < @{ $missing_resellers->{names} }; ++$i) {
    my $name = $missing_resellers->{names}->[$i];
    my $id = $missing_resellers->{ids}->[$i];
    push @resellers, $name;
    $reseller_ids->{$name} = $id;
    $reseller_names->{$id} = $name;
}

my $mark = NGCP::CDR::Export::get_mark($dbh, $collid, [ keys $reseller_names ]);
foreach my $reseller(@resellers) {
    $reseller_lines->{$reseller} //= {};
    my $reseller_contract_id = "";
    unless($reseller eq "system") {
        $reseller_contract_id = "-".$reseller_ids->{$reseller};
    }
    unless($mark->{"lastseq".$reseller_contract_id}) {
        $mark->{"lastseq".$reseller_contract_id} = 0;
    }
    my $file_idx = $mark->{"lastseq".$reseller_contract_id} // 0;
    my %lines = %{ $reseller_lines->{$reseller} };
    my @vals = map { $lines{$_} } sort { int($a) <=> int($b) } keys %lines;
    my $rec_idx = int(@vals);
    my $max = $config->{'default.MAX_ROWS_PER_FILE'} // $rec_idx;
    my $reseller_dname = $reseller . "/" . $dname;
    if($reseller ne "system") {
        $reseller_dname = "resellers/$reseller_dname";
    }
    my $reseller_tempdir = $tempdir . "/" . $reseller_dname;

    do {
        my $recs = ($rec_idx > $max) ? $max : $rec_idx;

        $file_idx++;
        my @filevals = @vals[0 .. $recs-1];
        @vals = @vals[$recs .. @vals-1];

        my $err;
        -d $reseller_tempdir || File::Path::make_path($reseller_tempdir, {error => \$err});
        if(defined $err && @$err) {
            DEBUG "!!! failed to create directory $reseller_tempdir: " . Dumper $err;
        }

        NGCP::CDR::Export::write_file(
            \@filevals, $reseller_tempdir, $config->{'default.PREFIX'},
            $config->{'default.VERSION'}, $file_ts, $file_idx, $config->{'default.SUFFIX'},
        );
        $rec_idx -= $recs;

    } while($rec_idx > 0);

    opendir(my $fh, $reseller_tempdir);
    foreach my $file(readdir($fh)) {
        my $src = "$reseller_tempdir/$file";
        my $dst = $config->{'default.CDRDIR'} . "/$reseller_dname/$file";
        if(-f $src) {
            DEBUG "### moving $src to $dst\n";
            my $err;
            -d $config->{'default.CDRDIR'} . "/$reseller_dname" || 
                File::Path::make_path($config->{'default.CDRDIR'} . "/$reseller_dname", {error => \$err});
            if(defined $err && @$err) {
                DEBUG "!!! failed to create directory $reseller_dname: " . Dumper $err;
            }
            unless(copy($src, $dst)) {
                DEBUG "!!! failed to move $src to $dst: $!\n";
            } else {
                DEBUG "### successfully moved $src to final destination $dst\n";
            }
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
            }
        }
    }
    NGCP::CDR::Export::set_mark($dbh, $collid, { "lastseq$reseller_contract_id" => $file_idx });
    close($fh);
}

NGCP::CDR::Export::update_export_status($dbh, "accounting.cdr", \@ids, "ok");
# TODO: should be tagged as ignored/skipped/whatever
NGCP::CDR::Export::update_export_status($dbh, "accounting.cdr", \@ignored_ids, "ok");

$dbh->commit or die("failed to commit db changes: " . $dbh->errstr);

