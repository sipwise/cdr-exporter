package NGCP::CDR::Transfer;

use File::Basename;
use Net::SFTP::Foreign;

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

    my $dst = $dir . "/" . basename($src);

    print "### transferring $src to $user\@$host:$port at $dst\n";
    $sftp->put($src, $dst);
}

1;
