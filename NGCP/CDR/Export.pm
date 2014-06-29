package NGCP::CDR::Export;

use Digest::MD5;

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
        $files_owner, $files_group, $files_mask
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
    chownmod($fn, $files_owner, $files_group, 0666, $files_mask);
}

1;
# vim: set tabstop=4 expandtab:


