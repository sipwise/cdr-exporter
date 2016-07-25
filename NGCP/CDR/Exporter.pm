package NGCP::CDR::Exporter;

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
use NGCP::CDR::Transfer;
use Data::Dumper;

BEGIN {
	require Exporter;
	our @ISA = qw(Exporter);
	our @EXPORT = qw(DEBUG confval write_reseller write_reseller_id update_export_status);
}

our $debug = 0;
our $collid = "exporter";

our @admin_fields;
our @reseller_fields;
my @joins;
my @conditions;
my $dbh;
my $q;
my $sth;
my %reseller_names;
my %reseller_ids;
my %reseller_lines;
my %mark;
my $dname;
my $tempdir;
my $file_ts;
my @reseller_positions;

# default config values
my %config = (
    'default.FILTER_FLAPPING' => 0,
    'default.MERGE_UPDATE' => 0,
    'default.PREFIX' => 'ngcp',
    'default.VERSION' => '007',
    'default.SUFFIX' => 'cdr',
    'default.FILES_OWNER' => 'cdrexport',
    'default.FILES_GROUP' => 'cdrexport',
    'default.FILES_MASK' => '022',
    'default.TRANSFER_TYPE' => "none",
    'default.TRANSFER_PORT' => 22,
    'default.TRANSFER_USER' => "cdrexport",
    'default.TRANSFER_KEY' => "/root/.ssh/id_rsa",
    'default.TRANSFER_REMOTE' => "/home/jail/home/cdrexport",
    'default.QUOTES' => "'"
);

sub DEBUG {
    say join (' ', @_);
}

my @config_paths = (qw#
    /etc/ngcp-cdr-exporter/
    .
#);

sub config2array {
    my $config_key = shift;
    my $val = confval($config_key);
    ref($val) eq 'ARRAY' and return @$val;
    return $val;
}

sub get_config {
	my ($coll, $cf, $conf_upd) = @_;

	$collid = $coll;

	for my $key (%$conf_upd) {
		$config{'default.' . $key} = $$conf_upd{$key};
	}

	my $config_file;
	foreach my $cp(@config_paths) {
	    if(-f "$cp/$cf") {
		$config_file = "$cp/$cf";
		last;
	    }
	}
	die "Config file $cf not found in path " . (join " or ", @config_paths) . "\n"
	    unless $config_file;

	Config::Simple->import_from("$config_file" , \%config) or
	    die "Couldn't open the configuration file '$config_file'.\n";

	# backwards compat
	$config{'default.DESTDIR'} //= $config{'default.CDRDIR'} // $config{'default.EDRDIR'};

	die "Invalid destination directory '".$config{'default.DESTDIR'}."'\n"
	    unless(-d $config{'default.DESTDIR'});

	foreach my $f(config2array('ADMIN_EXPORT_FIELDS')) {
	    $f =~ s/^#.+//; next unless($f);
	    $f =~ s/^\'//; $f =~ s/\'$//;
	    push @admin_fields, $f;
	}
	foreach my $f(config2array('RESELLER_EXPORT_FIELDS')) {
	    $f =~ s/^#.+//; next unless($f);
	    $f =~ s/^\'//; $f =~ s/\'$//;
	    push @reseller_fields, $f;
	}

	foreach my $f(@{confval('EXPORT_JOINS')}) {
	    $f =~ s/^\s*\{?\s*//; $f =~ s/\}\s*\}\s*$/}/;
	    my ($a, $b) = split('\s*=>\s*{\s*', $f);
	    $a =~ s/^\s*\'//; $a =~ s/\'$//g;
	    $b =~ s/\s*\}\s*$//;
	    my ($c, $d) = split('\s*=>\s*', $b);
	    $c =~ s/^\s*\'//g; $c =~ s/\'\s*//;
	    $d =~ s/^\s*\'//g; $d =~ s/\'\s*//;
	    push @joins, { $a => { $c => $d } };
	}

	if(confval('EXPORT_FAILED') eq "no") {
		push @conditions, { 'accounting.cdr.call_status' => { '=' => '"ok"' } };
	}
	if(confval('EXPORT_UNRATED') eq "no") {
		push @conditions, { 'accounting.cdr.rating_status' => { '=' => '"ok"' } };
	}
	foreach my $f(@{confval('EXPORT_CONDITIONS')}) {
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
}


sub confval {
	my ($val) = @_;
	return $config{'default.' . $val};
}

sub prepare_dbh {
	my ($trailer, $table) = @_;

	$dbh = DBI->connect("dbi:mysql:" . confval('DBDB') .
		";host=".confval('DBHOST'),
		confval('DBUSER'), confval('DBPASS'))
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
	foreach my $f(@$trailer) {
	    my ($key, $val) = %{ $f };
	    push @trail, "$key $val";
	}

	# extract positions of reseller fields from admin fields
	my %reseller_index;
	@reseller_index{@admin_fields} = (0..$#admin_fields);
	for(my $i = 0; $i < @reseller_fields; $i++) {
	    my $name = $reseller_fields[$i];
	    unless(exists $reseller_index{$name}) {
		die "Invalid RESELLER_EXPORT_FIELDS element '$name', not available in ADMIN_EXPORT_FIELDS!";
	    }
	    push @reseller_positions, $reseller_index{$name};
	}

	$q = "select " .
	    join(", ", @admin_fields) . " from $table " .
	    join(" ", @intjoins) . " " .
	    "where " . join(" and ", @conds) . " " .
	    join(" ", @trail);

	DEBUG $q if $debug;

}

sub prepare_output {
	my $tempfh = File::Temp->newdir(undef, CLEANUP => 1);
	$tempdir = $tempfh->dirname;

	my $now = time();
	my @now = localtime($now);
	$file_ts = NGCP::CDR::Export::get_ts_for_filename(\@now);

	my $full_name = (defined confval('FULL_NAMES') && confval('FULL_NAMES') eq "yes" ? 1 : 0);
	my $monthly_dir = (defined confval('MONTHLY_DIR') && confval('MONTHLY_DIR') eq "yes" ? 1 : 0);
	my $daily_dir = (defined confval('DAILY_DIR') && confval('DAILY_DIR') eq "yes" ? 1 : 0);
	$dname = "";
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
}

sub run {
	my ($cb) = @_;

	my $sth = $dbh->prepare($q);
	$sth->execute();
	while(my $row = $sth->fetchrow_arrayref) {
		my @res_row = @$row[@reseller_positions];
		$cb->($row, \@res_row);
	}
}

sub write_reseller {
	my ($reseller, $line) = @_;
	push(@{$reseller_lines{$reseller}}, $line);
	write_wrap($reseller);
}

sub write_reseller_id {
	my ($id, $line) = @_;
        if(!exists $reseller_names{$id}) {
            $reseller_names{$id} = NGCP::CDR::Export::get_reseller_name($dbh, $id);
            $reseller_ids{$reseller_names{$id}} = $id;
        }
        write_reseller($reseller_names{$id}, $line);
}

sub write_wrap {
    my ($reseller, $force) = @_;
    $force //= 0;
    $reseller_lines{$reseller} //= [];
    my $vals = $reseller_lines{$reseller};
    my $rec_idx = @$vals;
    my $max = confval('MAX_ROWS_PER_FILE') // $rec_idx;
    ($force == 0 && $rec_idx <= $max) and return;
    ($force == 1 && $rec_idx == 0) and return;
    my $reseller_contract_id = "";
    my $mark_query = undef;
    unless($reseller eq "system") {
        $reseller_contract_id = "-".$reseller_ids{$reseller};
	$mark_query = [ $reseller_ids{$reseller} ];
    }
    if (!defined($mark{"lastseq".$reseller_contract_id})) {
        my $tmpmark = NGCP::CDR::Export::get_mark($dbh, $collid, $mark_query);
	%mark = ( %mark, %$tmpmark );
        $mark{"lastseq".$reseller_contract_id} //= 0;
    }
    my $file_idx = $mark{"lastseq".$reseller_contract_id} // 0;
    my $reseller_dname = $reseller . "/" . $dname;
    if($reseller ne "system") {
        $reseller_dname = "resellers/$reseller_dname";
    }
    my $reseller_tempdir = $tempdir . "/" . $reseller_dname;

    do {
        my $recs = ($rec_idx > $max) ? $max : $rec_idx;

        $file_idx++;
        my @filevals = @$vals[0 .. $recs-1];
        @$vals = @$vals[$recs .. @$vals-1]; # modified $reseller_lines

        my $err;
        -d $reseller_tempdir || File::Path::make_path($reseller_tempdir, {error => \$err});
        if(defined $err && @$err) {
            DEBUG "!!! failed to create directory $reseller_tempdir: " . Dumper $err;
        }

        NGCP::CDR::Export::write_file(
            \@filevals, $reseller_tempdir, confval('PREFIX'),
            confval('VERSION'), $file_ts, $file_idx, confval('SUFFIX'),
        );
        $rec_idx -= $recs;

    } while($rec_idx > 0);

    opendir(my $fh, $reseller_tempdir);
    foreach my $file(readdir($fh)) {
        my $src = "$reseller_tempdir/$file";
        my $dst = confval('DESTDIR') . "/$reseller_dname/$file";
        if(-f $src) {
            DEBUG "### moving $src to $dst\n";
            my $err;
            -d confval('DESTDIR') . "/$reseller_dname" ||
                File::Path::make_path(confval('DESTDIR') . "/$reseller_dname", {error => \$err});
            if(defined $err && @$err) {
                DEBUG "!!! failed to create directory $reseller_dname: " . Dumper $err;
            }
            unless(copy($src, $dst)) {
                DEBUG "!!! failed to move $src to $dst: $!\n";
            } else {
                DEBUG "### successfully moved $src to final destination $dst\n";
            }
            NGCP::CDR::Export::chownmod($dst, confval('FILES_OWNER'),
                confval('FILES_GROUP'), oct(666),
                confval('FILES_MASK'));
            if(confval('TRANSFER_TYPE') eq "sftp") {
                NGCP::CDR::Transfer::sftp(
                    $dst, confval('TRANSFER_HOST'),
                    confval('TRANSFER_PORT'),
                    confval('TRANSFER_REMOTE'),
                    confval('TRANSFER_USER'),
                    confval('TRANSFER_PASS'),
                );
            } elsif(confval('TRANSFER_TYPE') eq "sftp-sh") {
                NGCP::CDR::Transfer::sftp_sh(
                    $dst, confval('TRANSFER_HOST'),
                    confval('TRANSFER_PORT'),
                    confval('TRANSFER_REMOTE'),
                    confval('TRANSFER_USER'),
                    confval('TRANSFER_KEY'),
                );
            }
        }
    }
    $mark{"lastseq".$reseller_contract_id} = $file_idx;
    NGCP::CDR::Export::set_mark($dbh, $collid, { "lastseq$reseller_contract_id" => $file_idx });
    close($fh);
}

sub finish {
	my @resellers = keys %reseller_lines;
	for my $reseller (@resellers) {
	    write_wrap($reseller, 1);
	}

	# we write empty cdrs for resellers which didn't have a call during this
	# export run, so get them into the list
	my $missing_resellers = NGCP::CDR::Export::get_missing_resellers($dbh, [ keys %reseller_names ]);
	for(my $i = 0; $i < @{ $missing_resellers->{names} }; ++$i) {
	    my $name = $missing_resellers->{names}->[$i];
	    my $id = $missing_resellers->{ids}->[$i];
	    push @resellers, $name;
	    $reseller_ids{$name} = $id;
	    $reseller_names{$id} = $name;
	    write_wrap($name, 2);
	}
}

sub update_export_status {
	NGCP::CDR::Export::update_export_status($dbh, @_);
}

sub commit {
	$dbh->commit or die("failed to commit db changes: " . $dbh->errstr);
}

1;

# vim: set tabstop=4 expandtab: