package NGCP::CDR::Transfer;

use File::Basename;
use File::Temp;
use Net::SFTP::Foreign;
use IPC::System::Simple qw/capturex/;

sub sftp_sh {
    my ($src, $host, $port, $dir, $user, $key) = @_;

    my $fname = basename($src);
    print "### transferring $src to $user\@$host:$port at $dir/$fname via sftp-sh\n";

    my $fh = File::Temp->new(UNLINK => 0);
    print $fh "cd $dir\nput $src $fname";
    my $cmd = "/usr/bin/sftp -b ".$fh->filename." -P $port -i $key $user\@$host";
    print "### using command $cmd\n";

    capturex([0], split(" ", $cmd));
}


sub sftp {
    my ($src, $host, $port, $dir, $user, $pass) = @_;

    my $sftp = Net::SFTP::Foreign->new(
        host => $host,
        port => $port,
        user => $user,
        password => $pass,
        timeout => 3,
	#password_prompt => qr/password:/,
    );
    if($sftp->error) {
        die "+++ failed to transfer $src to $user\@$host:$port/$dir: " . $sftp->error . "\n";
    }

    my $fname = basename($src);
    print "### transferring $src to $user\@$host:$port at $dir/$fname\n";
    $sftp->setcwd($dir);
    $sftp->put($src, $fname);
}

1;
