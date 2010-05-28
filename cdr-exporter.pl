#!/usr/bin/perl
# $Id: cdr-exporter.pl 943 2008-10-10 10:52:59Z agranig $

use strict;
use warnings;
use DBI;
use Getopt::Long;
use Digest::MD5;

my %CONF;

{
	my $res = GetOptions(
				"help"		=> \$CONF{help},
				"h=s"		=> \$CONF{host},
				"P=i"		=> \$CONF{port},
				"u=s"		=> \$CONF{user},
				"p=s"		=> \$CONF{pass},
				"d=s"		=> \$CONF{db},
				"t=s"		=> \$CONF{target},
				"f=s"		=> \$CONF{prefix},
				"v=s"		=> \$CONF{version},
	);

	if (!$res || $CONF{help}) {
		print("Usage:   $0 { --help | [-h HOSTNAME] [-P PORT] [-u USERNAME] [-p PASSWORD]\n");
		print("             -d DATABASE -t TARGETPATH -f PREFIX -v VERSION }\n");
		print("Example: $0 -d accounting -t /tmp/acc.$$ -f sipwise -v 001\n");
		exit($res ? 0 : 1);
	}

	for my $k (qw(db target prefix version)) {
		if (!defined($CONF{$k})) {
			print("Missing argument \"$k\", see $0 --help\n");
			exit(1);
		}
	}
}

-d $CONF{target} or die("Target directory $CONF{target} doesn't exist, stop");

$0 = "$0";	# hide command line with password

my $DBH = DBI->connect("dbi:mysql:$CONF{db}" . ($CONF{host} ? ";host=$CONF{host}" : ""), $CONF{user} || $ENV{USER} || undef, $CONF{pass}) or die("DB connect failed, stop");

print("+++ Start run with DB " . ($CONF{user} || "(undef)") . "\@$CONF{db} to $CONF{prefix}\n");

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

{
	my $ts = sprintf('%04i%02i%02i%02i%02i%02i', $NOW[5] + 1900, $NOW[4] + 1, @NOW[3,2,1,0]);
	my $limit = 5000;
	my $firstseq = $MARKS{lastseq};

	for (;;) {
		print("--- Starting CDR export with id > $MARKS{lastid}\n");
		my $s = $DBH->prepare(<<"!");
			select
				id,			update_time,
				source_user_id,		source_provider_id,
				source_user,		source_domain,
				source_cli,		source_clir,
				destination_user_id,	destination_provider_id,
				destination_user,	destination_domain,
				destination_user_in,	destination_domain_in,
				call_type,		call_status,
				call_code,		start_time,
				duration,		call_id,
				rating_status,		rated_at,
				carrier_cost,		reseller_cost,
				customer_cost,		carrier_billing_zone_id,
				reseller_billing_zone_id, customer_billing_zone_id,
				carrier_billing_fee_id,	reseller_billing_fee_id,
				customer_billing_fee_id,
				destination_user_dialed
			from
				cdr
			where
				id > ?
			order by
				id
			limit
				$limit
!
		$s->execute($MARKS{lastid}) or die($DBH->errstr);

		my @F;
		while (my $r = $s->fetch) {
			$MARKS{lastid} = $r->[0];
			my $l = join(",", map {(!defined($_) || $_ eq "") ? "" : "'$_'"} @$r);
			push(@F, $l);
		}

		if (!@F && $MARKS{lastseq} != $firstseq) {
			print("### No more data\n");
			last;
		}
		my $num = scalar(@F);
		unshift(@F, sprintf('%04i', $num));

		$MARKS{lastseq}++;
		my $fn = sprintf('%s/%s_%s_%s_%010i.cdr', $CONF{target}, $CONF{prefix}, $CONF{version}, $ts, $MARKS{lastseq});
		my $tfn = sprintf('%s/%s_%s_%s_%010i.cdr.'.$$, $CONF{target}, $CONF{prefix}, $CONF{version}, $ts, $MARKS{lastseq});
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
