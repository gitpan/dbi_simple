package DBI::Simple;

# $Id: $
# $Author: mbatista $
# Purpose: SQL dbi wrapper

use 5.008;
use strict;
use warnings;
use Carp;
use DBI;

require Exporter;

our @ISA = qw(Exporter);

our %EXPORT_TAGS = ( 'all' => [ qw(
	
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(
	
);

our $VERSION = '1.03';

sub new {#{{{
    my ( $proto, %d ) = @_;
    my $class = ref($proto) || $proto;
    my $self = { HOST => 'localhost',
                 UID  => 'root',
                 PWD  => '',
                 DB   => '',
                 DBH  => undef,
    };

    foreach ( keys %d ) {
        $self->{$_} = $d{$_} if defined $d{$_};
    }

    bless( $self, $class );
    $self->{DBH} = $self->connect();
    return $self;
}#}}}

### GET/SET FUNCTIONS ###
sub set {#{{{
    my $self = shift;
    my %d = @_;
    foreach ( keys %d ) {
        $self->{$_} = $d{$_} if defined $d{$_};
    }
    return $self;
}#}}}

sub host {#{{{
    my $self = shift;
    if (@_) { $self->{HOST} = shift; }
    return $self->{HOST};
}#}}}

sub uid {#{{{
    my $self = shift;
    if (@_) { $self->{UID} = shift; }
    return $self->{UID};
}#}}}

sub pwd {#{{{
    my $self = shift;
    if (@_) { $self->{PWD} = shift; }
    return $self->{PWD};
}#}}}

sub db {#{{{
    my $self = shift;
    if (@_) { $self->{DB} = shift; }
    return $self->{DB};
}#}}}

sub dbh {#{{{
    my $self = shift;
    return $self->{DBH};
}#}}}

#connect to DB using DBI
sub connect {#{{{
    my ($self) = @_;
    if ( not defined $self->{DBH} ) {
        $self->{DBH} = DBI->connect(
               "DBI:mysql:host=" . $self->host() . ";database=" . $self->db(),
               $self->uid(), $self->pwd() )
            or die $!;
    }
    return $self->{DBH};
}#}}}

## execute a command
sub do {#{{{
    my ( $self, $insert ) = @_;

    return $self->connect->do($insert);
}#}}}

# process a file
sub process {#{{{
    my $self = shift;
    if (@_) {
        my $file = shift;
        open SQL, "<$file";
        my @data = <SQL>;
        close SQL;
        chomp @data;

        my $do = join( '', @data );
        for ( split( "\;", $do ) ) {
            $self->connect->do($_);
        }
    } else {
        return 0;
    }
    return 1;
}#}}}

## get tables in DB
sub tables {#{{{
    my $self = shift;
    return $self->dbh->selectcol_arrayref("show tables");
}#}}}

## get columns in a table
sub columns {#{{{
    my ( $self, $table ) = @_;
    return $self->dbh->selectcol_arrayref("SHOW COLUMNS FROM $table");
}#}}}

## get list of a col
sub selectcol {#{{{
    my ( $self, $select ) = @_;
    return $self->dbh->selectcol_arrayref($select) or die "$! : $select";
}#}}}

## get query into array or arrayrefs
sub query {#{{{
    my $self = shift;
    my $select = shift;
    my $s = $self->dbh->prepare($select);
    $s->execute(@_) or die "$! : $select";
    my $all = $s->fetchall_arrayref;
    return $all;
}#}}}

sub query_columns {#{{{
    my $self = shift;
    my $select = shift;
    return &rows_to_columns($self->query($select, @_));
}#}}}


## get query into hash#{{{
sub query_hash {
    my ( $self, $select ) = @_;
    my $s = $self->dbh->prepare($select);
    $s->execute() or die "$! : $select";
    my %hash;
    while (my $hash_ref = $s->fetchrow_hashref){
        foreach (keys %$hash_ref){
            push( @{$hash{$_}}, ${$hash_ref}{$_});
        }
    }
    return \%hash;
}#}}}

## get query into hash#{{{
sub list_hash {
    my $self = shift;
    my $select = shift;
    my $s = $self->dbh->prepare($select);
    $s->execute(@_) or die "$! : $select";
    my @list;
    while (my $hash_ref = $s->fetchrow_hashref){
            push( @list, $hash_ref);
    }
    return \@list;
}#}}}


#creates insert and inserts
sub insert {#{{{
    my $self = shift;
    my ( $table, $columns, $values ) = @_;
    $values  = &addQuotes($values);
    $columns = &addBackTics($columns);

    my $insert = "INSERT IGNORE INTO `$table` (";
    $insert .= join( ",", @$columns);
    $insert .= ") VALUES (";
    $insert .= join( ",", @$values );
    $insert .= ")";
    
    return $self->dbh()->do($insert) or die $!;
}#}}}

#helper to add quotes ''
sub addQuotes {#{{{
    my ($list) = @_;
    foreach (@$list) {
        $_ = '' if not defined $_;
        $_ =~ s/^\ //;
        $_ = "\"$_\"" unless ($_ =~ /FROM_UNIXTIME/ or $_ =~ /NOW\(\)/i);
    }
    return $list;
}#}}}

#helper to add bactics ``
sub addBackTics {#{{{
    my ($list) = @_;
    foreach (@$list) {
        $_ = '' if not defined $_;
        $_ =~ s/^\ //;
        $_ = "\`$_\`";
    }
    return $list;
}#}}}


## insert hash
sub insert_hash {#{{{
    my $self = shift;
    my ($table, $hash) = @_;
    my $cols;
    my $vals;

    foreach (keys %$hash){
        push (@$cols, $_);
        push (@$vals, ${$hash}{$_});
    }
    return $self->insert($table, $cols, $vals);
}#}}}

## UPDATE
sub update {#{{{
    my $self = shift;
    my ($host, $set, $where) = @_;
    
    my $update = "UPDATE $host SET " . &set_hash($set);
       $update .= " WHERE " . &set_hash($where) 
        if &set_hash($where) ne '';

   return $self->dbh()->do($update) or die $!;
}#}}}

sub DESTROY {#{{{
    my $self = shift;

#    $self->dbh->disconnect();
}#}}}

# HELPER FUNCTIONS#{{{
## HELPER#{{{
# convert fetchall (rows) to columns for graphs
sub rows_to_columns {
    my $rows = shift;
    my @columns;

    my $c = 0;
    for my $row (@$rows) {
        $c = 0;
        for my $col (@$row) {
            push( @{ $columns[$c] }, $col );
            $c++;
        }
    }
    return \@columns;
}#}}}
sub set_hash {#{{{
    my $set = shift;
    my @sets;

    foreach my $key (keys %$set){
        $set->{$key} = '' if not defined $set->{$key};
        if ($set->{$key} =~ /\(|\)/){
            push (@sets, "`$key` = $set->{$key}");
        }else{
            push (@sets, "`$key` = '$set->{$key}'");
        }
    }
    return join(" and ", @sets);
}#}}}#}}}



1;

__END__

=head1 NAME

DBI::Simple - Perl extension for DBI 

=head1 SYNOPSIS

  use DBI::Simple;

=head1 ABSTRACT

  DBI::Simple - Perl extension for DBI

=head1 DESCRIPTION

B<DBI::Simple> is a I<perl5> module to simplify the use of DBI. 

=head1 USAGE

Create a new dbh connection.

use DBI::Simple;
my $dbh = new DBI::Simple(
    DB   => $database,
    HOST => $host,
    UID  => $user,
    PWD  => $passwd
);

=head1 METHODS

=over 4

=item $dbh->set(%options)

Set the dbh connection if not done in constructor.

=item $dbh->host($host)

Get or set the host for the dbh connection.

=item $dbh->uid($uid)

Get or set the uid for the dbh connection.

=item $dbh->pwd($pwd)

Get or set the password for the dbh connection.

=item $dbh->db($db)

Get or set the database for the dbh connection.

=item $dbh->dbh()

Get the dbh for the dbhh connection.

=item $dbh->connect()

Reconnect to databse.

=item $dbh->do($sql_statment)

Execute any sql command.

=item $dbh->process($file)

Process contents of a file.
SQL statements are split by semicolons.

=item $dbh->tables()

Get the list of tables in the current database.
return type array reference.

=item $dbh->columns($table)

Get the columns in $table. 
return type array reference.

=item $dbh->selectcol($select)

Get the first column from $select.
return type array reference.

=item $dbh->query($select)

Get query into a list of array references.
return type array reference.

=item $dbh->query_columns($select)

converts $dbh->query (rows) into columns.
this is usefull for GD::Graphs

=item $dbh->query_hash($select)

Get query into a hash with list of values.

USE: 
    ## RETURNS A HASH WITH THE COLUMN AS VALUE
    my $query_hash  = $dbh->query_hash('select * from test');
    foreach my $column (keys %$query_hash){ 
        my @column_data = $query_hash->{$column};
        ## DO SOMETHING WITH THE DATA IN THE COLUMN
        print "$column : @column_data\n";
    }
return type hash reference.

=item $dbh->list_hash($select)

Get query into list of hashes.

USE:
    ## RETURNS A LIST OF HASHES
    my $list_hash = $dbh->list_hash('select * from test');
    foreach my $hash (@$list_hash){
        ## DO SOMETHING WITH THE HASH
        foreach my $key (keys %$hash){
           ##      KEY : VALUE
           print "$key : $hash->{$key} \n"; 
        }
    }
return type array reference.

=item $dbh->insert($table, $columns, $values)

Takes list of coluumns (array reference) and list of values (array reference) and insertes it into the given $table.

USE:
    $dbh->insert('test', ['col1', 'col2'], ['val1', 'val2']);

=item $dbh->insert_hash($table, $hash)

Inserts the given hash reference into the given $table.

USE:
    $dbh->insert_hash('test', {col1 => 'val1', col2 => 'val2'});

=item $dbh->update($table, $set, $where)

parameters: string, hash_ref, hash_ref 
Updates the given data in $table.

USE:
    $dbh->update(
        'test',                        # TABLE
        { col1 => 'new_val1',          # SET col TO new_val1 .. 
          col2 => 'new_val2'},     
        { colx => 'val',               # WHERE colx EQUALS val .. 
          coly => 'val'}               # THIS IS OPTIONAL
    );

=head1 SEE ALSO

perldoc DBI::Simple
perldoc DBI

=head1 AUTHOR

Manny Batista, E<lt>mbatista@ccs.neu.edu<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2007 by Manny Batista

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself. 

=cut
