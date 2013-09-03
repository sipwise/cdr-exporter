#!/usr/bin/perl
# $Id: cdr-exporter.pl 943 2008-10-10 10:52:59Z agranig $

use strict;
use warnings;
use DBI;
use Digest::MD5;

our $DBHOST;
our $DBUSER;
our $DBPASS;
our $DBDB;
our $CDRDIR;
our $PREFIX;
our $VERSION;
our $DAILY_DIR;
our $MONTHLY_DIR;
our $FULL_NAMES;
our $EXPORT_UNRATED;
our $EXPORT_INCOMING;
our $EXPORT_FAILED;
our $FILES_OWNER = 'cdrexport';
our $FILES_GROUP = 'cdrexport';
our $FILES_MASK = '022';


my $config_file = "/etc/ngcp-cdr-exporter/cdr-exporter.conf";
open CONFIG, "$config_file" or die "Program stopping, couldn't open the configuration file '$config_file'.\n";

while (<CONFIG>) {
    chomp;                  # no newline
    s/#.*//;                # no comments
    s/^\s+//;               # no leading white
    s/\s+$//;               # no trailing white
    next unless length;     # anything left?
    my ($var, $value) = split(/\s*=\s*/, $_, 2);
        no strict 'refs';
        $$var = $value;
}
close CONFIG;





sub chownmod {
	my ($file, $user, $group, $defmode, $mask) = @_;

	if ($user || $group) {
		my @arg = (-1, -1, $file);
		$user and $arg[0] = getpwnam($user) || -1;
		$group and $arg[1] = getgrnam($group) || -1;
		chown(@arg);
	}
	$mask and chmod($defmode & ~oct($mask), $file);
}




my $DBH = DBI->connect("dbi:mysql:$DBDB;host=$DBHOST", $DBUSER, $DBPASS);

$DBH or return 0;
print("+++ Start run with DB " . ($DBUSER || "(undef)") . "\@$DBDB to $PREFIX\n");

my $COLLID = "exporter";
my %MARKS;	# last seq etc
{
	my $s = $DBH->prepare("select acc_id from mark where collector = ?");
	for my $mk (qw(lastid lastseq)) {
		$s->execute("$COLLID-$mk") or die($DBH->errstr);
		my $r = $s->fetch;
		$MARKS{$mk} = ($r && $r->[0]) ? $r->[0] : 0;
	}
}

my $NOW = time();
my @NOW = localtime($NOW);

my @CDR_BODY_FIELDS = qw(
	id update_time 
	source_user_id source_provider_id source_external_subscriber_id 
	source_subscriber_id source_external_contract_id source_account_id 
	source_user source_domain source_cli source_clir source_ip 
	destination_user_id destination_provider_id destination_external_subscriber_id 
	destination_subscriber_id destination_external_contract_id destination_account_id 
	destination_user destination_domain
	destination_user_in destination_domain_in destination_user_dialed
	peer_auth_user peer_auth_realm 
	call_type call_status call_code 
	init_time start_time duration
	call_id rating_status rated_at 
	source_carrier_cost source_customer_cost 
	source_carrier_zone source_customer_zone
	source_carrier_destination source_customer_destination 
	source_carrier_free_time source_customer_free_time
	destination_carrier_cost destination_customer_cost 
	destination_carrier_zone destination_customer_zone
	destination_carrier_destination destination_customer_destination 
	destination_carrier_free_time destination_customer_free_time
	source_reseller_cost
	source_reseller_zone
	source_reseller_destination
	source_reseller_free_time
	destination_reseller_cost
	destination_reseller_zone
	destination_reseller_destination
	destination_reseller_free_time
);
my @CDR_RESELLER_BODY_FIELDS = qw(
	id update_time 
	source_user_id source_provider_id source_external_subscriber_id 
	source_subscriber_id source_external_contract_id source_account_id 
	source_user source_domain source_cli source_clir source_ip 
	destination_user_id destination_provider_id destination_external_subscriber_id 
	destination_subscriber_id destination_external_contract_id destination_account_id 
	destination_user destination_domain
	destination_user_in destination_domain_in destination_user_dialed
	peer_auth_user peer_auth_realm 
	call_type call_status call_code 
	init_time start_time duration
	call_id rating_status rated_at 
	source_customer_cost 
	source_customer_zone
	source_customer_destination 
	source_customer_free_time
	destination_customer_cost 
	destination_customer_zone
	destination_customer_destination 
	destination_customer_free_time
);

{
	my ($dir1, $dir2, $ts);
	$ts = sprintf('%04i%02i%02i%02i%02i%02i', $NOW[5] + 1900, $NOW[4] + 1, @NOW[3,2,1,0]);
	$FULL_NAMES = ($FULL_NAMES && $FULL_NAMES =~ /1|y/i);

	if ($MONTHLY_DIR && $MONTHLY_DIR =~ /1|y/i) {
		$dir1 = sprintf('%04i%02i', $NOW[5] + 1900, $NOW[4] + 1);
		if ($DAILY_DIR && $DAILY_DIR =~ /1|y/i) {
			$dir2 = sprintf('%02i', $NOW[3]);
			$FULL_NAMES or $ts = sprintf('%02i%02i%02i', @NOW[2,1,0]);
		}
		else {
			$dir2 = '.';
			$FULL_NAMES or $ts = sprintf('%02i%02i%02i%02i', @NOW[3,2,1,0]);
		}
	}
	elsif ($DAILY_DIR && $DAILY_DIR =~ /1|y/i) {
		$dir1 = sprintf('%04i%02i%02i', $NOW[5] + 1900, $NOW[4] + 1, $NOW[3]);
		$dir2 = '.';
		$FULL_NAMES or $ts = sprintf('%02i%02i%02i', @NOW[2,1,0]);
	}
	else {
		$dir1 = $dir2 = '.';
	}

	my $limit = 5000;
	my $firstseq = $MARKS{lastseq};

	my $reseller_name_sth = $DBH->prepare('select name from billing.resellers where id = ?');

	for (;;) {
		print("--- Starting CDR export\n");
		my @ids = ();
		my $s = $DBH->prepare("
			select	cdr.id,			update_time,
				source_user_id,		source_provider_id,
				source_external_subscriber_id,	source_bvs.id AS source_subscriber_id,
				source_external_contract_id,	source_account_id,
				source_user,		source_domain,
				source_cli,		source_clir, source_ip,
				destination_user_id,	destination_provider_id,
				destination_external_subscriber_id,	destination_bvs.id AS destination_subscriber_id,
				destination_external_contract_id,	destination_account_id,
				destination_user,	destination_domain,
				destination_user_in,	destination_domain_in, destination_user_dialed,
				peer_auth_user,		peer_auth_realm,
				call_type,		call_status,
				call_code,		CONCAT(FROM_UNIXTIME(start_time), '.', SUBSTRING_INDEX(start_time, '.', -1)) AS start_time,
				CONCAT(FROM_UNIXTIME(init_time), '.', SUBSTRING_INDEX(init_time, '.', -1)) AS init_time,
				duration,		call_id,
				rating_status,		rated_at,
				source_carrier_cost,	source_reseller_cost, source_customer_cost,		
				source_carrier_free_time,	source_reseller_free_time,  source_customer_free_time,
				source_carrier_bbz.zone AS source_carrier_zone, source_reseller_bbz.zone AS source_reseller_zone,
				source_customer_bbz.zone AS source_customer_zone, source_carrier_bbz.detail AS source_carrier_destination,
				source_reseller_bbz.detail AS source_reseller_destination, source_customer_bbz.detail AS source_customer_destination,
				destination_carrier_cost,	destination_reseller_cost, destination_customer_cost,		
				destination_carrier_free_time,	destination_reseller_free_time,  destination_customer_free_time,
				destination_carrier_bbz.zone AS destination_carrier_zone, destination_reseller_bbz.zone AS destination_reseller_zone,
				destination_customer_bbz.zone AS destination_customer_zone, destination_carrier_bbz.detail AS destination_carrier_destination,
				destination_reseller_bbz.detail AS destination_reseller_destination, destination_customer_bbz.detail AS destination_customer_destination
			from	accounting.cdr
				LEFT JOIN billing.billing_zones_history source_carrier_bbz ON cdr.source_carrier_billing_zone_id = source_carrier_bbz.id
				LEFT JOIN billing.billing_zones_history source_reseller_bbz ON cdr.source_reseller_billing_zone_id = source_reseller_bbz.id
				LEFT JOIN billing.billing_zones_history source_customer_bbz ON cdr.source_customer_billing_zone_id = source_customer_bbz.id
				LEFT JOIN billing.billing_zones_history destination_carrier_bbz ON cdr.destination_carrier_billing_zone_id = destination_carrier_bbz.id
				LEFT JOIN billing.billing_zones_history destination_reseller_bbz ON cdr.destination_reseller_billing_zone_id = destination_reseller_bbz.id
				LEFT JOIN billing.billing_zones_history destination_customer_bbz ON cdr.destination_customer_billing_zone_id = destination_customer_bbz.id
				LEFT JOIN billing.voip_subscribers source_bvs ON cdr.source_user_id = source_bvs.uuid
				LEFT JOIN billing.voip_subscribers destination_bvs ON cdr.destination_user_id = destination_bvs.uuid
			where	cdr.export_status = 'unexported' AND cdr.id > ?
		". ($EXPORT_INCOMING eq 'yes' ? '' : "and source_provider_id = 1") ."
		". ($EXPORT_FAILED eq 'yes' ? '' : "and call_status = 'ok'") ."
			order by
				cdr.id
			limit	$limit
		");

		$s->execute($MARKS{lastid}) or die($DBH->errstr);

		my (@F, %R, %RNAME);
		while (my $r = $s->fetchrow_hashref()) {
			# finish export to give rate-o-mat time to catch up
			if ($r->{rating_status} eq 'unrated') {
				last if $EXPORT_UNRATED !~ /y|1|true/i;
			}
			else {
				unless(defined $r->{source_carrier_zone}) { # platform internal, no peering cost calculated
					$r->{source_carrier_cost} = '0.00';
					$r->{source_carrier_zone} = 'onnet';
					$r->{source_carrier_destination} = 'platform internal';
				}
			}

			my $l = join(",", map {(!defined($_) || $_ eq "") ? "''" : "'$_'"} @$r{@CDR_BODY_FIELDS});
			push(@F, $l);
			push(@ids, $r->{id});

			my $l_r = join(",", map {(!defined($_) || $_ eq "") ? "''" : "'$_'"} @$r{@CDR_RESELLER_BODY_FIELDS});
			my %rid_used;
			for my $dir (qw(source destination)) {
				my $xuid = $r->{$dir . '_user_id'};
				$xuid or next;

				my $rid = $r->{$dir . '_provider_id'};
				$rid or next;
				$rid_used{$rid} and next;
				$rid_used{$rid} = 1;

				my $rname = $RNAME{$rid};
				if (!defined($rname)) {
					$reseller_name_sth->execute($rid);
					($rname) = $reseller_name_sth->fetchrow_array;
					if (!$rname) {
						$RNAME{$rid} = '';
						next;
					}
					$rname =~ s,/,_,gs;
					$rname =~ s,\0,,gs;
					$RNAME{$rid} = $rname;
				}
				$rname eq '' and next;

				push(@{$R{$rname}}, $l_r);
			}
		}

		if (!@F && $MARKS{lastseq} != $firstseq) {
			print("### No more data\n");
			last;
		}

		my $num = scalar(@F);

		$MARKS{lastseq}++;

		for my $ref ([\@F, 'system'], (map {[$R{$_}, 'resellers', $_]} keys(%R))) {
			my ($f, @dirs) = @$ref;

			my $num = scalar(@$f);
			unshift(@$f, sprintf('%s,%04i', $VERSION, $num));

			my $dircomp = $CDRDIR;
			my @dirlist;
			for my $dirpart (@dirs, $dir1, $dir2) {
				$dircomp .= "/$dirpart";
				push(@dirlist, $dircomp);
			}

			for my $dd (@dirlist) {
				if (! -d $dd) {
					mkdir($dd) or die("failed to create target directory $dd ($!), stop");
					chownmod($dd, $FILES_OWNER, $FILES_GROUP, 0777, $FILES_MASK);
				}
			}
			my $fn = sprintf('%s/%s_%s_%s_%010i.cdr', $dircomp, $PREFIX, $VERSION, $ts, $MARKS{lastseq});
			my $tfn = sprintf('%s/%s_%s_%s_%010i.cdr.'.$$, $dircomp, $PREFIX, $VERSION, $ts, $MARKS{lastseq});
			my $fd;
			open($fd, ">", $tfn) or die("failed to open tmp-file $tfn ($!), stop");
			my $ctx = Digest::MD5->new;

			for my $l (@$f) {
				my $ol = "$l\n";
				print $fd ($ol);
				$ctx->add($ol);
			}

			my $md5 = $ctx->hexdigest;
			print $fd ("$md5\n");

			print("### $num data lines written to $tfn, checksum is $md5\n");
			close($fd) or die ("failed to close tmp-file $tfn ($!), stop");
			undef($ctx);

			rename($tfn, $fn) or die("failed to move tmp-file $tfn to $fn ($!), stop");
			print("### successfully moved $tfn to $fn\n");
			chownmod($fn, $FILES_OWNER, $FILES_GROUP, 0666, $FILES_MASK);
		}

		# update exported cdrs
		my $ex_sth = $DBH->prepare("UPDATE cdr SET export_status='ok', exported_at=NOW() ".
						"WHERE id IN (".('?,' x $#ids)."?)");
		$ex_sth->execute(@ids) or die($DBH->errstr);

		$num < $limit and last;
	}

	# we don't update the lastid key anymore, as we're now checking for export_status; the lastid check above
	# is really just for upgrade scenarios to make sure there is no race condition between an updated
	# exporter is running before ngcp-update-db-schema is executed
	delete $MARKS{lastid};
	for my $mk (keys(%MARKS)) {
		# race me...
		my $aff = $DBH->do("update mark set acc_id = ? where collector = ?", undef, $MARKS{$mk}, "$COLLID-$mk");
		defined($aff) or die("failed to update DB mark, stop");
		if ($aff == 0) {
			$DBH->do("insert into mark (collector, acc_id) values (?,?)", undef, "$COLLID-$mk", $MARKS{$mk}) or die("failed to update DB mark, stop");
		}
	}
	print("::: Updated DB marks, all done.\n");
}
