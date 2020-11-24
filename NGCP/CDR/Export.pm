package NGCP::CDR::Export;

use Digest::MD5;
use DateTime;
use warnings;
use strict;

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
        $s->execute("$name-$mk") or die($dbh->errstr . "\n");
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
        $s->execute("$name-$mk") or die($dbh->errstr . "\n");
        my $r = $s->fetch;
        if($r && defined $r->[0]) {
            $u->execute($mark->{$mk}, "$name-$mk");
        } else {
            $i->execute("$name-$mk", $mark->{$mk});
        }
    }
}

sub update_export_status {
    my ($dbh, $tbl, $ids, $status) = @_;
    return unless(@{ $ids });
    while (my @chunk = splice @$ids, 0, 10000) {
        my $sth = $dbh->prepare("update $tbl set export_status=?, exported_at=now()" .
          " where id in (" . substr(',?' x scalar @chunk,1) . ")");
        $sth->execute($status, @chunk) or die($dbh->errstr . "\n");
        $sth->finish();
    }
}

sub upsert_export_status {
    my ($dbh, $stream, $tbl, $estbl, $ids, $status) = @_;
    return unless(@{ $ids });
    while (my @chunk = splice @$ids, 0, 10000) {
        my $sth = $dbh->prepare("insert into $estbl " .
          "select _cdr.id,_cesc.id,now(),\"$status\",_cdr.start_time from $tbl _cdr " .
          "join (select * from accounting.cdr_export_status where type = \"$stream\") as _cesc " .
          "where _cdr.id in (" . substr(',?' x scalar @chunk,1) . ") " .
          "on duplicate key update export_status = \"$status\", exported_at = now()");
        $sth->execute(@chunk) or die($dbh->errstr . "\n");
        $sth->finish();
    }
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
    my $stmt = "select name, $reseller_id_col from billing.resellers where status != \"terminated\"";
    if(@{ $cids }) {
        $stmt .= " and $reseller_id_col not in (" . join (',', map { '?' }(1 .. @{ $cids }) ) . ")";
    }
    my $sth = $dbh->prepare($stmt);
    $sth->execute(@{ $cids });
    my @names = ();
    my @ids = ();
    while(my $res = $sth->fetchrow_arrayref) {
        push @names, $res->[0];
        push @ids, $res->[1];
    }
    $sth->finish();
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
        $user and ($arg[0] = (getpwnam($user) || -1));
        $group and ($arg[1] = (getgrnam($group) || -1));
        chown(@arg);
    }
    $mask and chmod($defmode & ~oct($mask), $file);
}

sub write_file {
    my (
        $lines, $dircomp, $prefix, $version, $ts, $lastseq, $suffix,
        $format, $file_data, $csv_header, $csv_footer
    ) = @_;

    my $fn =  sprintf('%s/%s_%s_%s_%010i.%s', $dircomp, $prefix, $version, $ts, $lastseq, $suffix);
    my $tfn = sprintf('%s/%s_%s_%s_%010i.%s.'.$$, $dircomp, $prefix, $version, $ts, $lastseq, $suffix);
    my $fd;
    open($fd, ">", $tfn) or die("failed to open tmp-file $tfn ($!), stop\n");
    my $ctx = Digest::MD5->new;

    my $num = @{ $lines };
    if ($format eq 'kabelplus') {
        unshift(@{ $lines }, "'$num'". ',' x 15 ."'hdr',,,'".($$file_data[0]//'')."','".($$file_data[1]//'')."',,,'".($$file_data[2]//'')."'," .
                "'".($$file_data[3]//'')."','".($$file_data[4]//'')."'". ',' x 10);
    } else {
        my $str =
            apply_format($csv_header, {
                            rows      => $num,
                            version   => $version,
                            checksum  => undef,
                            first_seq => $lastseq,
                            last_seq  => $lastseq+$num,
                            _ts       => $ts,
            });
        unshift(@{ $lines }, $str) if $str;
    }

    my $nl = "\n";
    $format eq 'kabelplus' and $nl = "\r\n";

    for my $l (@{ $lines }) {
        my $ol = "$l$nl";
        print $fd ($ol);
        $ctx->add($ol);
    }

    my $md5 = $ctx->hexdigest;
    if ($format eq 'kabelplus') {
        print $fd (",,'$md5'". ',' x 13 ."'md5'". ',' x 19 . "$nl");
    } else {
        my $str =
            apply_format($csv_footer, {
                            rows      => $num,
                            version   => $version,
                            checksum  => $md5,
                            first_seq => $lastseq,
                            last_seq  => $lastseq+$num,
                            _ts       => $ts,
            });
        print $fd "$str$nl" if $str;
    }

    NGCP::CDR::Exporter::DEBUG("$num data lines written to $tfn, checksum is $md5\n");
    close($fd) or die ("failed to close tmp-file $tfn ($!), stop\n");
    undef($ctx);

    rename($tfn, $fn) or die("failed to move tmp-file $tfn to $fn ($!), stop\n");
    NGCP::CDR::Exporter::DEBUG("successfully moved $tfn to $fn\n");
}

sub apply_format {
    my ($str, $data) = @_;

    return unless $str;

    my @m_formats = ();
    my $applied = $str;

    my @dt_seq = $data->{_ts} =~ /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/;
    my $dt_seq_idx = 0;
    my $dt = DateTime->new(map { $_ => $dt_seq[$dt_seq_idx++] }
                                 qw(year month day hour minute second) );

    while ($str =~ /(?<!\\)(\$\{([^\$\[\]]+)\})/) {
        my $m = {
            'mac' => $1,
            'inp' => $2,
            'pos' => $-[1],
            'len' => length($1),
        };
        substr($str, $m->{pos}, $m->{len}) = "##$m->{inp}#";
        push @m_formats, $m;
    }

    foreach my $m (reverse @m_formats) {
        my ($mac, $inp, $pos, $len) = @{$m}{qw(mac inp pos len)};
        my ($name,$strf) = split(/,/, $inp);

        my $out = '';
        if ($name eq 'datetime') { # special handling
            $out = $dt->strftime($strf // '%Y-%m-%d %H:%M:%S');
        } elsif (defined $data->{$name}) {
            $out = sprintf($strf // '%s', $data->{$name});
        }

        substr($applied, $pos, $len) = $out;
    }

    return $applied;
}

1;
# vim: set tabstop=4 expandtab:
