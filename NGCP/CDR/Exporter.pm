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
use Sys::Syslog;
use Proc::ProcessTable qw();

BEGIN {
    require Exporter;
    our @ISA = qw(Exporter);
    our @EXPORT = qw(
        DEBUG
        find_processes
        import_config
        prepare_config
        confval
        quote_field
        write_reseller
        write_reseller_id
        build_query
        load_preferences
        apply_sclidui_rwrs
        prefval
        prepare_output
        run
        finish
        update_export_status
        upsert_export_status
        ilog
    );
}

my $exporter_type = "exporter";

my $last_admin_field;
our @admin_fields;
our @reseller_fields;
our @data_fields;
my @joins;
my @conditions;
my $dbh;
my $q;
my %reseller_names;
my %reseller_ids;
my %reseller_lines;
my %reseller_file_data;
my %reseller_counts;
my %reseller_file_counts;
my %mark;
my $start_ts;
my $dname_map;
my $tempdir;
my $file_ts_map;
my @reseller_positions;
my @data_positions;

my $stream = "default";
# default config values
my %config = (
    'default.FILTER_FLAPPING' => 0,
    'default.MERGE_UPDATE' => 0,
    'default.ENABLED' => "yes",
    'default.INTERMEDIATE' => "no",
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
    'default.QUOTES' => "'",
    'default.CSV_SEP' => ',',
    'default.CSV_ESC' => "\\",
    'default.CSV_HEADER' => '${version},${lines,%04i}',
    'default.CSV_FOOTER' => '${checksum}',
    'default.WRITE_EMPTY' => "yes",
);

# specify 'system' default reseller preferences:
my $reseller_preferences = {};
# eg. $reseller_preferences->{'system'}->{'attribute y'} = 'z';

my $rewrite_rule_sets = {};

my $field_positions = {
    source_cli => {
        aliases => [ qw(source_cli
                        accounting.cdr.source_cli
                        cdr.source_cli
                        accounting.int_cdr.source_cli
                        int_cdr.source_cli
                        base_table.source_cli) ],
        admin_positions => undef,
        reseller_positions => undef,
    },
    destination_user_in => {
        aliases => [ qw(destination_user_in
                        accounting.cdr.destination_user_in
                        cdr.destination_user_in
                        accounting.int_cdr.destination_user_in
                        int_cdr.destination_user_in
                        base_table.destination_user_in) ],
        admin_positions => undef,
        reseller_positions => undef,
    },
};

sub DEBUG {
    ilog('debug', @_);
}
sub ERR {
    ilog('err', @_);
}

my @config_paths = (qw#
    /etc/ngcp-cdr-exporter/
    .
#);
#/home/rkrenn/temp/cdrexportstreams/

sub config2array {
    my $config_key = shift;
    my $val = confval($config_key);
    ref($val) eq 'ARRAY' and return @$val;
    return $val;
}

sub get_config_fields {
    my ($name) = @_;
    my @ret;
    foreach my $f(config2array($name)) {
        $f or next;
        $f =~ s/^#.+//; next unless($f);
        $f =~ s/^\'//; $f =~ s/\'$//;
        push @ret, $f;
    }
    return @ret;
}

sub find_processes {
    my $re = shift;
    my $pt = Proc::ProcessTable->new();
    my @result = ();
    foreach my $p (@{$pt->table}){
        if ($p->cmndline =~ $re) {
            push(@result, {
                pid => $p->pid,
                start => $p->start,
            });
        }
    }
    return @result;
}

sub import_config {

    my $cf = shift;
    my $config_file;
    foreach my $cp (@config_paths) {
        my $path = $cp;
        $path .= '/' if $path !~ m!/$!;
        $path .= $cf;
        if(-f $path) {
            $config_file = $path;
            last;
        }
    }
    die "Config file $cf not found in path " . (join " or ", @config_paths) . "\n"
        unless $config_file;

    Config::Simple->import_from("$config_file" , \%config) or
        die "Couldn't open the configuration file '$config_file'.\n";

    start_log();

    my $cfg = new Config::Simple();
    $cfg->read($config_file);

    my %presets = %config;

    my @blocks = qw(default);
    foreach my $block ($cfg->get_block()) {
        next if 'default' eq $block;
        push(@blocks,$block);
        die "Stream name 'ama_ccs' is reserved.\n" if 'ama_ccs' eq $block;
        die "Stream name 'exporter' is reserved.\n" if 'exporter' eq $block;
        die "Stream name 'eventexporter' is reserved.\n" if 'exporter' eq $block;
        foreach my $key (%presets) {
            my $preset = $config{$key};
            if ($key =~ /^default\.(.+)$/) {
                $config{$block . '.' . $1} = $preset unless exists $config{$block . '.' . $1};
            }
        }
    }
    return @blocks;

}

sub prepare_config {
    ($exporter_type, $stream, my $conf_upd) = @_;

    $stream //= 'default';

    if (defined $conf_upd) {
        for my $key (keys %$conf_upd) {
            my $upd = $$conf_upd{$key};
            if ('CODE' eq ref $upd) {
                $config{$stream . '.' . $key} = &$upd($config{$stream . '.' . $key});
            } else {
                $config{$stream . '.' . $key} = $$conf_upd{$key};
            }
        }
    }

    # backwards compat
    $config{$stream . '.DESTDIR'} //= $config{$stream . '.CDRDIR'} // $config{$stream . '.EDRDIR'};

    if ($stream ne 'default') {
        # use constants from default, if missing:
        $config{$stream . '.PREFIX'} //= $config{'default.PREFIX'};
        $config{$stream . '.DBDB'} //= $config{'default.DBDB'};
        $config{$stream . '.VERSION'} //= $config{'default.VERSION'};
    }

    #test overrides:
    #$config{$stream . '.DBHOST'} = '192.168.0.29';
    #$config{$stream . '.DBUSER'} = 'root';
    #$config{$stream . '.DBPASS'} = '';
    #$config{$stream . '.TRANSFER_REMOTE'} = "/home/rkrenn/temp/cdrexportstreams/cdrexport";
    #$config{$stream . '.DESTDIR'} = "/home/rkrenn/temp/cdrexportstreams/cdrexport";

    die "Invalid destination directory '".$config{$stream . '.DESTDIR'}."'\n"
        unless(-d $config{$stream . '.DESTDIR'});

    @admin_fields = get_config_fields('ADMIN_EXPORT_FIELDS');
    @reseller_fields = get_config_fields('RESELLER_EXPORT_FIELDS');
    @data_fields = get_config_fields('DATA_FIELDS');

    @joins = ();
    foreach my $f (get_config_fields('EXPORT_JOINS')) {
        $f =~ s/^\s*\{?\s*//; $f =~ s/\}\s*\}\s*$/}/;
        my ($a, $b) = split(/\s*=>\s*{\s*/, $f);
        $a =~ s/^\s*\'//; $a =~ s/\'$//g;
        $b =~ s/\s*\}\s*$//;
        my ($c, $d) = split(/\s*=>\s*/, $b);
        $c =~ s/^\s*\'//g; $c =~ s/\'\s*//;
        $d =~ s/^\s*\'//g; $d =~ s/\'\s*//;
        push @joins, { $a => { $c => $d } };
    }

    @conditions = ();
    foreach my $f (get_config_fields('EXPORT_CONDITIONS')) {
        next unless($f);
        $f =~ s/^\s*\{?\s*//; $f =~ s/\}\s*\}\s*$/}/;
        my ($a, $b) = split(/\s*=>\s*{\s*/, $f);
        $a =~ s/^\s*\'//; $a =~ s/\'$//g;
        $b =~ s/\s*\}\s*$//;

        my ($c, $d) = split(/\s*=>\s*/, $b);
        $c =~ s/^\s*\'//g; $c =~ s/\'\s*//;
        $d =~ s/^\s*\'//g; $d =~ s/\'\s*//;
        push @conditions, { $a => { $c => $d } };
    }

    %reseller_names = ();
    %reseller_ids = ();
    %reseller_lines = ();
    %reseller_file_data = ();
    %reseller_counts = ();
    %reseller_file_counts = ();
    %mark = ();

    if ((confval("MAINTENANCE") // 'no') eq 'yes') {
        exit(0);
    }
}


sub confval {
    my ($val) = @_;
    return $config{$stream . '.' . $val};
}

sub quote_field {
    my ($field,$sep,$quotes,$escape_symbol) = @_;
    if (defined $quotes and length($quotes) > 0) {
        if (defined $escape_symbol and length($escape_symbol) > 0) {
            if (defined $field and length($field) > 0) {
                foreach my $escape ($escape_symbol,$quotes) {
                    my $escape_re = quotemeta($escape); #fun
                    $field =~ s/($escape_re)/$escape_symbol$1/g;
                }
                $field = $quotes . $field . $quotes;
            } else {
                $field = $quotes . $quotes;
            }
        } else {
            if (defined $field and length($field) > 0) {
                $field = $quotes . $field . $quotes;
            } else {
                $field = $quotes . $quotes;
            }
        }
    } else {
        if (defined $escape_symbol and length($escape_symbol) > 0) {
            if (defined $field and length($field) > 0) {
                foreach my $escape ($escape_symbol,$sep) {
                    my $escape_re = quotemeta($escape); #fun
                    $field =~ s/($escape_re)/$escape_symbol$1/g;
                }
            } else {
                $field = '';
            }
        } else {
            if (not defined $field or length($field) == 0) {
                $field = '';
            }
        }
    }
    return $field;

}

sub extract_field_positions {
    my (@fields) = @_;
    # extract positions of data fields from admin fields
    my %index;
    my @positions;
    @index{@admin_fields} = (0..$#admin_fields);
    for(my $i = 0; $i < @fields; $i++) {
        my $name = $fields[$i];
        my $position;
        if (! exists $index{$name}) {
            push(@admin_fields, $name);
            $position = $#admin_fields;
        } else {
            $position = $index{$name};
        }
        push(@positions, $position);
        foreach my $af (keys %$field_positions) {
            $field_positions->{$af}->{admin_positions} = {}
                unless defined $field_positions->{$af}->{admin_positions};
            $field_positions->{$af}->{reseller_positions} = {}
                unless defined $field_positions->{$af}->{reseller_positions};
            if (grep { lc($_) eq lc($name); } @{$field_positions->{$af}->{aliases}}) {
                $field_positions->{$af}->{admin_positions}->{$position} = 1;
                $field_positions->{$af}->{reseller_positions}->{$i} = 1;
            }
        }
    }
    return @positions;
};

sub build_query {
    my ($trailer, $table, $prepend_default_cond_code) = @_;

    unless ($dbh) {
        $dbh = DBI->connect("dbi:mysql:" . confval('DBDB') .
            ";host=".confval('DBHOST'),
            confval('DBUSER'), confval('DBPASS'))
            or die "failed to connect to db: $DBI::errstr";
        $dbh->{AutoCommit} = 0;
    }

    my @intjoins = ();
    my @conds = ();

    if ('CODE' eq ref $prepend_default_cond_code) {
        &$prepend_default_cond_code($dbh,\@intjoins,\@conds);
    }

    foreach my $f(@joins) {
        my ($table, $keys) = %{ $f };
        my ($foreign_key, $own_key) = %{ $keys };
        push @intjoins, "left outer join $table on $foreign_key = $own_key";
    }

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

    $last_admin_field = $#admin_fields;
    foreach my $af (keys %$field_positions) {
        undef $field_positions->{$af}->{admin_positions};
        undef $field_positions->{$af}->{reseller_positions};
    }
    @reseller_positions = extract_field_positions(@reseller_fields);
    @data_positions = extract_field_positions(@data_fields);
    foreach my $af (keys %$field_positions) {
        $field_positions->{$af}->{admin_positions} = [
            sort keys %{$field_positions->{$af}->{admin_positions}}
        ] if defined $field_positions->{$af}->{admin_positions};
        $field_positions->{$af}->{reseller_positions} = [
            sort keys %{$field_positions->{$af}->{reseller_positions}}
        ] if defined $field_positions->{$af}->{reseller_positions};
    }

    $q = "select " .
        join(", ", @admin_fields) . " from $table base_table " .
        join(" ", @intjoins) . " " .
        "where " . join(" and ", @conds) . " " .
        join(" ", @trail);

    DEBUG $q; # if $debug;

}

sub load_preferences {

    my $stmt = "select r.contract_id,a.attribute,a.max_occur,v.value " .
        "from billing.resellers r " .
        "join provisioning.voip_reseller_preferences v on v.reseller_id = r.id " .
        "join provisioning.voip_preferences a on a.id = v.attribute_id";
    my $sth = $dbh->prepare($stmt);
    $sth->execute();
    while(my $res = $sth->fetchrow_arrayref) {
        my ($reseller_id,$attribute,$max_occur,$value) = @$res;
        $reseller_preferences->{$reseller_id} = {} unless exists $reseller_preferences->{$reseller_id};
        my $preferences = $reseller_preferences->{$reseller_id};
        if ($max_occur > 1) {
            $preferences->{$attribute} = [] unless exists $preferences->{$attribute};
            $preferences->{$attribute} = [ $preferences->{$attribute} ] unless 'ARRAY' eq ref $preferences->{$attribute};
            push(@{$preferences->{$attribute}},$value);
        } else {
            $preferences->{$attribute} = $value unless exists $preferences->{$attribute}; # use only if no default pref is defined
        }
        if ($attribute eq 'cdr_export_sclidui_rwrs_id'
            and $preferences->{$attribute}) {
            load_rewrite_rules($preferences->{$attribute});
        }
    }
    $sth->finish();

}

sub load_rewrite_rules {

    my ($rwrs_id) = @_;
    return if exists $rewrite_rule_sets->{$rwrs_id};
    my $stmt = "select * from provisioning.voip_rewrite_rules where set_id = ? order by priority asc";
    my $sth = $dbh->prepare($stmt);
    $sth->execute($rwrs_id);
    my %rules = ();
    while (my $rule = $sth->fetchrow_hashref) {
        next unless $rule->{enabled}; # panel does not consider enabled?
        $rules{$rule->{direction}} = {} unless exists $rules{$rule->{direction}};
        my $directions = $rules{$rule->{direction}};
        $directions->{$rule->{field}} = [] unless exists $directions->{$rule->{field}};
        my $fields = $directions->{$rule->{field}};
        push(@$fields,$rule);
    }
    $rewrite_rule_sets->{$rwrs_id} = \%rules;
    $sth->finish();
    return;

}

sub apply_sclidui_rwrs {
    my ($reseller_id,$row,$position_offset) = @_;
    $position_offset //= 0;
    my $row_type;
    if ('system' eq $reseller_id) {
        $row_type = 'admin_positions';
    } else {
        $row_type = 'reseller_positions';
    }
    if (defined $field_positions->{source_cli}->{$row_type}) {
        foreach my $i (@{$field_positions->{source_cli}->{$row_type}}) {
            $row->[$i + $position_offset] = apply_rewrite(
                number => $row->[$i + $position_offset],
                dir => 'caller_out',
                rwrs_id => prefval($reseller_id,'cdr_export_sclidui_rwrs_id') // 0,
            );
        }
    }
    if (defined $field_positions->{destination_user_in}->{$row_type}) {
        foreach my $i (@{$field_positions->{destination_user_in}->{$row_type}}) {
            $row->[$i + $position_offset] = apply_rewrite(
                number => $row->[$i + $position_offset],
                dir => 'callee_out',
                rwrs_id => prefval($reseller_id,'cdr_export_sclidui_rwrs_id') // 0,
            );
        }
    }
    return @$row;
}

sub apply_rewrite {
    my (%params) = @_;

    my $callee = $params{number};
    my $dir = $params{dir};
    my $rwrs_id = $params{rwrs_id};

    return $callee unless $dir =~ /^(caller_in|callee_in|caller_out|callee_out|callee_lnp|caller_lnp)$/;

    my ($field, $direction) = split /_/, $dir;

    my @rules;
    if ($rwrs_id and exists $rewrite_rule_sets->{$rwrs_id}) {
        my $rwrs_rules = $rewrite_rule_sets->{$rwrs_id};
        if ($direction and exists $rwrs_rules->{$direction}) {
            my $directions = $rwrs_rules->{$direction};
            if ($field and exists $directions->{$field}) {
                @rules = @{$directions->{$field}};
            }
        }
    }

    foreach my $r (@rules) {
        my $match = $r->{match_pattern};
        my $replace = $r->{replace_pattern};

        #print ">>>>>>>>>>> match=$match, replace=$replace\n";

        $match = [ $match ] if(ref $match ne "ARRAY");

        $replace = shift @{ $replace } if(ref $replace eq "ARRAY");
        $replace =~ s/\\(\d{1})/\${$1}/g;

        $replace =~ s/\"/\\"/g;
        $replace = qq{"$replace"};

        my $found;
        #print ">>>>>>>>>>> apply matches\n";
        foreach my $m(@{ $match }) {
            #print ">>>>>>>>>>>     m=$m, r=$replace\n";
            if($callee =~ s/$m/$replace/eeg) {
                # we only process one match
                #print ">>>>>>>>>>> match found, callee=$callee\n";
                $found = 1;
                last;
            }
        }
        last if $found;
        #print ">>>>>>>>>>> done, match=$match, replace=$replace, callee is $callee\n";
    }

    return $callee;
}

sub prefval {
    my ($reseller_id, $attribute) = @_;
    if ($reseller_id and exists $reseller_preferences->{$reseller_id}) {
        my $preferences = $reseller_preferences->{$reseller_id};
        if ($attribute and exists $preferences->{$attribute}) {
            return $preferences->{$attribute};
        }
    }
    return;
}

sub prepare_output {

    $tempdir = File::Temp->newdir;

    $start_ts = time();

    $dname_map = {};
    $file_ts_map = {};

}

sub get_dir_ts {

    my ($reseller) = @_;

    if (not exists $dname_map->{$reseller} or not exists $file_ts_map->{$reseller}) {

        my @now;
        if (defined confval('DIR_RESELLER_TIME') && confval('DIR_RESELLER_TIME') eq "yes") {
            my $stmt;
            my @params = ($start_ts);
            if ($reseller eq "system") {
                $stmt = 'select convert_tz(from_unixtime(?),@@session.time_zone,(SELECT COALESCE((SELECT t.name FROM ngcp.timezone t LIMIT 1),@@global.time_zone))) as ts';
            } else {
                $stmt = 'select convert_tz(from_unixtime(?),@@session.time_zone,(select coalesce((select tz.name from billing.v_contract_timezone tz where contract_id = ? limit 1),@@global.time_zone))) as ts';
                push(@params,$reseller_ids{$reseller});
            }
            my $sth = $dbh->prepare('select second(start.ts),minute(start.ts),hour(start.ts),dayofmonth(start.ts),'.
                'month(start.ts)-1,year(start.ts)-1900,dayofweek(start.ts)-1,dayofyear(start.ts)-1 from (' . $stmt . ') as start');
            $sth->execute(@params); # or die($DBI::errstr);
            @now = $sth->fetchrow_array;
            $sth->finish;
        } else {
            @now = localtime($start_ts);
        }

        #a reseller must not have "system" as name
        $file_ts_map->{$reseller} = NGCP::CDR::Export::get_ts_for_filename(\@now);
        my $full_name = (defined confval('FULL_NAMES') && confval('FULL_NAMES') eq "yes" ? 1 : 0);
        my $monthly_dir = (defined confval('MONTHLY_DIR') && confval('MONTHLY_DIR') eq "yes" ? 1 : 0);
        my $daily_dir = (defined confval('DAILY_DIR') && confval('DAILY_DIR') eq "yes" ? 1 : 0);
        $dname_map->{$reseller} = '';
        if($monthly_dir && !$daily_dir) {
            $dname_map->{$reseller} .= sprintf("%04i%02i", $now[5] + 1900, $now[4] + 1);
            $full_name or $file_ts_map->{$reseller} = sprintf("%02i%02i%02i%02i", @now[3,2,1,0]);
        } elsif(!$monthly_dir && $daily_dir) {
            $dname_map->{$reseller} .= sprintf("%04i%02i%02i", $now[5] + 1900, $now[4] + 1, $now[3]);
            $full_name or $file_ts_map->{$reseller} = sprintf("%02i%02i%02i", @now[2,1,0]);
        } elsif($monthly_dir && $daily_dir) {
            $dname_map->{$reseller} .= sprintf("%04i%02i/%02i", $now[5] + 1900, $now[4] + 1, $now[3]);
            $full_name or $file_ts_map->{$reseller} = sprintf("%02i%02i%02i", @now[2,1,0]);
        }
    }

    return ($dname_map->{$reseller},$file_ts_map->{$reseller});

}

sub run {
    my ($cb) = @_;

    ilog('info', 'Started execution');

    my $rec_in = 0;
    my $sth = $dbh->prepare($q);
    $sth->execute() or die "Query failed: " . $sth->errstr;
    while(my $row = $sth->fetchrow_arrayref) {
        #print $rec_in ."\n";
        $rec_in++;
        my @admin_row = @$row[0 .. $last_admin_field];
        my @res_row = @$row[@reseller_positions];
        my @data_row = @$row[@data_positions];
        $cb->(\@admin_row, \@res_row, \@data_row);
        $dbh->ping() if ($rec_in % 10000 == 0);
    }
    $sth->finish();

    for my $key (keys(%reseller_counts)) {
        ilog('info', "Wrote $reseller_counts{$key} records for reseller $key");
    }
    for my $key (keys(%reseller_file_counts)) {
        ilog('info', "Created $reseller_counts{$key} files for reseller $key");
    }

    ilog('info', 'Finished processing records');

    return $rec_in;
}

sub write_reseller {
    my ($reseller, $line, $callback, $callback_arg) = @_;
    push(@{$reseller_lines{$reseller}}, $line);
    $callback and $callback->($callback_arg, \$reseller_file_data{$reseller});
    $reseller_counts{$reseller}++;
    write_wrap($reseller);
}

sub write_reseller_id {
    my ($id, $line, $callback, $callback_arg) = @_;
    if(!exists $reseller_names{$id}) {
        $reseller_names{$id} = NGCP::CDR::Export::get_reseller_name($dbh, $id);
        $reseller_ids{$reseller_names{$id}} = $id;
    }
    write_reseller($reseller_names{$id}, $line, $callback, $callback_arg);
}

sub write_wrap {
    my ($reseller, $force) = @_;
    $force //= 0;
    $reseller_lines{$reseller} //= [];
    my $vals = $reseller_lines{$reseller};
    my $rec_idx = @$vals;
    my $max = confval('MAX_ROWS_PER_FILE') // ($rec_idx + 1);
    ($force == 0 && $rec_idx < $max) and return;
    ($force == 1 && $rec_idx == 0) and return;
    my $reseller_contract_id = "";
    my $mark_query = undef;
    unless($reseller eq "system") {
        $reseller_contract_id = "-".$reseller_ids{$reseller};
        $mark_query = [ $reseller_ids{$reseller} ];
    }
    if (!defined($mark{"lastseq".$reseller_contract_id})) {
        my $tmpmark = NGCP::CDR::Export::get_mark($dbh,
            ($stream eq 'default' ? $exporter_type : ($exporter_type . '-' . $stream)), $mark_query);
        %mark = ( %mark, %$tmpmark );
        $mark{"lastseq".$reseller_contract_id} //= 0;
    }
    my $file_idx = $mark{"lastseq".$reseller_contract_id} // 0;
    my ($dname,$file_ts) = get_dir_ts($reseller);
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
            ERR "failed to create directory $reseller_tempdir: " . Dumper $err;
        }

        NGCP::CDR::Export::write_file(
            \@filevals, $reseller_tempdir, confval('PREFIX'),
            confval('VERSION'), $file_ts, $file_idx, confval('SUFFIX'),
            confval('FILE_FORMAT') // 'default', $reseller_file_data{$reseller},
            confval('CSV_HEADER'), confval('CSV_FOOTER')
        );
        $rec_idx -= $recs;
        delete($reseller_file_data{$reseller});
        $reseller_file_counts{$reseller}++;

    } while($rec_idx > 0);

    opendir(my $fh, $reseller_tempdir);
    foreach my $file(readdir($fh)) {
        my $src = "$reseller_tempdir/$file";
        my $dst = confval('DESTDIR') . "/$reseller_dname/$file";
        if(-f $src) {
            DEBUG "moving $src to $dst\n";
            my $err;
            -d confval('DESTDIR') . "/$reseller_dname" ||
                File::Path::make_path(confval('DESTDIR') . "/$reseller_dname", {
                        error => \$err,
                        user => confval('FILES_OWNER'),
                        group => confval('FILES_GROUP')
                    }
                );
            if(defined $err && @$err) {
                ERR "failed to create directory $reseller_dname: " . Dumper $err;
            }
            unless(move($src, $dst)) {
                ERR "failed to move $src to $dst: $!\n";
            } else {
                DEBUG "successfully moved $src to final destination $dst\n";
            }
            NGCP::CDR::Export::chownmod($dst, confval('FILES_OWNER'),
                confval('FILES_GROUP'), oct(666),
                confval('FILES_MASK'));
            if((confval('TRANSFER_TYPE') // '') eq "sftp") {
                NGCP::CDR::Transfer::sftp(
                    $dst, confval('TRANSFER_HOST'),
                    confval('TRANSFER_PORT'),
                    confval('TRANSFER_REMOTE'),
                    confval('TRANSFER_USER'),
                    confval('TRANSFER_PASS'),
                );
            } elsif ((confval('TRANSFER_TYPE') // '') eq "sftp-sh") {
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
    close($fh);
    $mark{"lastseq".$reseller_contract_id} = $file_idx;
    NGCP::CDR::Export::set_mark($dbh, ($stream eq 'default' ? $exporter_type : ($exporter_type . '-' . $stream)),
        { "lastseq$reseller_contract_id" => $file_idx });
}

sub finish {
    ilog('info', 'Finalizing output files');

    my @resellers = keys %reseller_lines;
    for my $reseller (@resellers) {
        write_wrap($reseller, 1);
    }

    return unless (defined confval('WRITE_EMPTY') && confval('WRITE_EMPTY') eq "yes");

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

sub upsert_export_status {
    NGCP::CDR::Export::upsert_export_status($dbh, $stream, @_);
}

sub commit {
    ilog('info', 'Committing changes to database');
    $dbh->commit or die("failed to commit db changes: " . $dbh->errstr);
    ilog('info', 'All done');
}

sub start_log {
    closelog();
    my $ident = confval("SYSLOG_IDENT") || $0;
    $ident =~ s/.*\///; # truncate path
    my $facl = confval("SYSLOG_FACILITY") || 'daemon';
    openlog($ident, 'pid,ndelay', $facl);

    $SIG{__WARN__} = sub { syslog('warning', @_); }; ## no critic
    $SIG{__DIE__} = sub { syslog('crit', @_); die(@_); }; ## no critic
}

sub ilog {
    syslog(@_);
}

INIT {
    start_log();
}

1;

# vim: set tabstop=4 expandtab:
