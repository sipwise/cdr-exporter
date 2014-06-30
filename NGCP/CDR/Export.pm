package NGCP::CDR::Export;

use Digest::MD5;

sub get_mark {
    my ($dbh, $name) = @_;
    my %marks = ();
    my $s = $dbh->prepare("select acc_id from accounting.mark where collector = ?");
    for my $mk (qw(lastid lastseq)) {
            $s->execute("$name-$mk") or die($dbh->errstr);
            my $r = $s->fetch;
            $marks{$mk} = ($r && $r->[0]) ? $r->[0] : 0;
    }
    return \%marks;
}

sub set_mark {
    my ($dbh, $name, $mark) = @_;
    my $s = $dbh->prepare("select acc_id from accounting.mark where collector = ?");
    my $i = $dbh->prepare("insert into accounting.mark (collector, acc_id) values(?,?)");
    my $u = $dbh->prepare("update accounting.mark set acc_id = ? where collector = ?");
    for my $mk (keys %{ $mark }) {
            $s->execute("$name-$mk") or die($dbh->errstr);
            my $r = $s->fetch;
            if($r && $r->[0]) {
                $u->execute($mark->{$mk}, "$name-$mk");
            } else {
                $i->execute("$name-$mk", $mark->{$mk});
            }
    }
}

sub update_export_status{
    my ($dbh, $tbl, $ids) = @_;
    return unless(@{ $ids });
    my $u = $dbh->prepare("update $tbl set export_status='ok', exported_at=now()" .
      " where id in (" . join (',', map { '?' }(1 .. @{ $ids }) ) . ")");
    $u->execute(@{ $ids }) or die($dbh->errstr);
}


sub get_ts_for_filename {
    my $now = time;
    my @now = localtime($now);
    return sprintf('%04i%02i%02i%02i%02i%02i', 
        $now[5] + 1900, $now[4] + 1, @now[3,2,1,0]);
}

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

sub write_file {
    my (
        $lines, $dircomp, $prefix, $version, $ts, $lastseq, $suffix, 
    ) = @_;
    my $fn =  sprintf('%s/%s_%s_%s_%010i.%s', $dircomp, $prefix, $version, $ts, $lastseq, $suffix);
    my $tfn = sprintf('%s/%s_%s_%s_%010i.%s.'.$$, $dircomp, $prefix, $version, $ts, $lastseq, $suffix);
    my $fd;
    open($fd, ">", $tfn) or die("failed to open tmp-file $tfn ($!), stop");
    my $ctx = Digest::MD5->new;

    my $num = @{ $lines };
    unshift(@{ $lines }, sprintf('%s,%04i', $version, $num));

    for my $l (@{ $lines }) {
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
}

1;
# vim: set tabstop=4 expandtab:


