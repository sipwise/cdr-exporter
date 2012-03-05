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
my %MARKS;	# last id etc
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

my @CDR_BODY_FIELDS = qw(id update_time source_user_id source_provider_id source_external_subscriber_id source_external_contract_id source_account_id source_user source_domain source_cli
                         source_clir destination_user_id destination_provider_id destination_external_subscriber_id destination_external_contract_id destination_account_id destination_user destination_domain
                         destination_user_in destination_domain_in peer_auth_user peer_auth_realm call_type call_status call_code start_time duration
                         call_id rating_status rated_at carrier_cost customer_cost carrier_zone customer_zone
                         carrier_destination customer_destination destination_user_dialed
			 reseller_cost carrier_free_time reseller_free_time customer_free_time reseller_zone
			 reseller_destination);

{
	my ($dir1, $dir2, $ts);
	$ts = sprintf('%04i%02i%02i%02i%02i%02i', $NOW[5] + 1900, $NOW[4] + 1, @NOW[3,2,1,0]);
	$FULL_NAMES = ($FULL_NAMES && $MONTHLY_DIR =~ /1|y/i);

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

	for (;;) {
		print("--- Starting CDR export with id > $MARKS{lastid}\n");
		my $s = $DBH->prepare(<<"!");
			select	cdr.id,			update_time,
				source_user_id,		source_provider_id,
				source_external_subscriber_id, source_external_contract_id,
				source_account_id,
				source_user,		source_domain,
				source_cli,		source_clir,
				destination_user_id,	destination_provider_id,
				destination_external_subscriber_id, destination_external_contract_id,
				destination_account_id
				destination_user,	destination_domain,
				destination_user_in,	destination_domain_in,
				peer_auth_user,		peer_auth_realm,
				call_type,		call_status,
				call_code,		CONCAT(FROM_UNIXTIME(start_time), '.', SUBSTRING_INDEX(start_time, '.', -1)) AS start_time,
				duration,		call_id,
				rating_status,		rated_at,
				carrier_cost,		reseller_cost,
				customer_cost,		frag_carrier_onpeak,
				frag_reseller_onpeak,   frag_customer_onpeak,
				carrier_free_time,	reseller_free_time,
				customer_free_time,
				carrier_bbz.zone AS carrier_zone, reseller_bbz.zone AS reseller_zone,
				customer_bbz.zone AS customer_zone, carrier_bbz.detail AS carrier_destination,
				reseller_bbz.detail AS reseller_destination, customer_bbz.detail AS customer_destination,
				destination_user_dialed
			from	accounting.cdr
				LEFT JOIN billing.billing_zones_history carrier_bbz ON cdr.carrier_billing_zone_id = carrier_bbz.id
				LEFT JOIN billing.billing_zones_history reseller_bbz ON cdr.reseller_billing_zone_id = reseller_bbz.id
				LEFT JOIN billing.billing_zones_history customer_bbz ON cdr.customer_billing_zone_id = customer_bbz.id
			where	cdr.id > ?
			  and	source_provider_id = 1
			  and	call_status = 'ok'
			order by
				cdr.id
			limit	$limit
!
		$s->execute($MARKS{lastid}) or die($DBH->errstr);

		my @F;
		while (my $r = $s->fetchrow_hashref()) {
			# finish export to give rate-o-mat time to catch up
			if ($r->{rating_status} eq 'unrated') {
				last if $EXPORT_UNRATED !~ /y|1|true/i;
			}
			else {
				unless(defined $r->{carrier_zone}) { # platform internal, no peering cost calculated
					$r->{carrier_cost} = '0.00';
					$r->{carrier_zone} = 'onnet';
					$r->{carrier_destination} = 'platform internal';
				}
			}

			$MARKS{lastid} = $r->{id};
			my $l = join(",", map {(!defined($_) || $_ eq "") ? "''" : "'$_'"} @$r{@CDR_BODY_FIELDS});
			push(@F, $l);
		}

		if (!@F && $MARKS{lastseq} != $firstseq) {
			print("### No more data\n");
			last;
		}
		my $num = scalar(@F);
		unshift(@F, sprintf('%s,%04i', $VERSION, $num));

		$MARKS{lastseq}++;
		for my $dd ("$CDRDIR/$dir1", "$CDRDIR/$dir1/$dir2") {
			if (! -d $dd) {
				mkdir($dd) or die("failed to create target directory $dd ($!), stop");
				chownmod($dd, $FILES_OWNER, $FILES_GROUP, 0777, $FILES_MASK);
			}
		}
		my $fn = sprintf('%s/%s/%s/%s_%s_%s_%010i.cdr', $CDRDIR, $dir1, $dir2, $PREFIX, $VERSION, $ts, $MARKS{lastseq});
		my $tfn = sprintf('%s/%s/%s/%s_%s_%s_%010i.cdr.'.$$, $CDRDIR, $dir1, $dir2, $PREFIX, $VERSION, $ts, $MARKS{lastseq});
		my $fd;
		open($fd, ">", $tfn) or die("failed to open tmp-file $tfn ($!), stop");
		my $ctx = Digest::MD5->new;

		for my $l (@F) {
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

		$num < $limit and last;
	}

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
