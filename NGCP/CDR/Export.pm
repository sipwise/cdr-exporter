package NGCP::CDR::Export;

use Digest::MD5;

our $reseller_id_col = 'contract_id';

sub get_mark {
    my ($dbh, $name, $resellers) = @_;
    my %marks = ();
    $resellers = [] unless defined($resellers);
    my $s = $dbh->prepare("select acc_id from accounting.mark where collector = ?");
    my @ids = qw/lastid lastseq/;
    foreach my $id(@{ $resellers }) {
        push @ids, "lastseq-$id";
    }
    for my $mk(@ids) {
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
            if($r && defined $r->[0]) {
                $u->execute($mark->{$mk}, "$name-$mk");
            } else {
                $i->execute("$name-$mk", $mark->{$mk});
            }
    }
}

sub update_export_status{
    my ($dbh, $tbl, $ids, $status) = @_;
    return unless(@{ $ids });
    my $u = $dbh->prepare("update $tbl set export_status=?, exported_at=now()" .
      " where id in (" . join (',', map { '?' }(1 .. @{ $ids }) ) . ")");
    $u->execute($status, @{ $ids }) or die($dbh->errstr);
}

sub get_reseller_name {
    my ($dbh, $cid) = @_;
    my $q = $dbh->prepare("select name from billing.resellers where $reseller_id_col = ?");
    $q->execute($cid);
    my $rname;
    ($rname) = $q->fetchrow_array;
    if (!$rname) {
        $rname = '';
    }
    $rname =~ s,/,_,gs;
    $rname =~ s,\0,,gs;
    return $rname;
}

sub get_missing_resellers {
    my ($dbh, $cids) = @_;
    my $qs = "select name, $reseller_id_col from billing.resellers";
    if(@{ $cids }) {
        $qs .= " where $reseller_id_col not in (" . join (',', map { '?' }(1 .. @{ $cids }) ) . ")";
    }
    my $q = $dbh->prepare($qs);
    $q->execute(@{ $cids });
    my @names = ();
    my @ids = ();
    while(my $res = $q->fetchrow_arrayref) {
        push @names, $res->[0];
        push @ids, $res->[1];
    }
    return { names => \@names, ids => \@ids };
}


sub get_ts_for_filename {
    my ($xnow) = @_;
    
    my $now; my @now;

    if(defined $xnow) {
        @now = @{ $xnow };
    } else {
        $now = time;
        @now = localtime($now);
    }
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
        $format, $file_data,
    ) = @_;

    my $fn =  sprintf('%s/%s_%s_%s_%010i.%s', $dircomp, $prefix, $version, $ts, $lastseq, $suffix);
    my $tfn = sprintf('%s/%s_%s_%s_%010i.%s.'.$$, $dircomp, $prefix, $version, $ts, $lastseq, $suffix);
    my $fd;
    open($fd, ">", $tfn) or die("failed to open tmp-file $tfn ($!), stop");
    my $ctx = Digest::MD5->new;

    my $num = @{ $lines };
    if ($format eq 'kabelplus') {
            unshift(@{ $lines }, "'$num'". ',' x 15 ."'hdr',,,'$$file_data[0]','$$file_data[1]',,,'$$file_data[2]'," .
			        "'$$file_data[3]','$$file_data[4]'". ',' x 10);
    }
    else {
            unshift(@{ $lines }, sprintf('%s,%04i', $version, $num));
    }

    my $nl = "\n";
    $format eq 'kabelplus' and $nl = "\r\n";

    for my $l (@{ $lines }) {
        my $ol = "$l$nl";
        print $fd ($ol);
        $ctx->add($ol);
    }

    my $md5 = $ctx->hexdigest;
    if ($fileformat eq 'kabelplus') {
            print $fd (",,'$md5'". ',' x 13 ."'md5'". ',' x 19 . "$nl");
    }
    else {
            print $fd ("$md5$nl");
    }

    print("### $num data lines written to $tfn, checksum is $md5\n");
    close($fd) or die ("failed to close tmp-file $tfn ($!), stop");
    undef($ctx);

    rename($tfn, $fn) or die("failed to move tmp-file $tfn to $fn ($!), stop");
    print("### successfully moved $tfn to $fn\n");
}

1;
# vim: set tabstop=4 expandtab:


