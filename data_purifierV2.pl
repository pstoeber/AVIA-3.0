use strict;
use warnings;
use DBI; #calling Perl Database Interface Package 

my @pureID; # initializing array to store queried Sample ID's
#connedting to RDS database
my $dbh = DBI -> connect("dbi:mysql:database=ExpressionDB;host=expressiondb.cosheh3pc1ze.us-east-2.rds.amazonaws.com:3306", "pstoeber", "Sk1ttles", {RaiseError => 1, AutoCommit => 1}) or die $DBI::errstr;  #database=ExpressionDB;host=expressiondb.cosheh3pc1ze.us-east-2.rds.amazonaws.com:3306
my $sql = "Select distinct sampid from gtex_master_view where smts = \"colon\" or smts = \"breast\" or smts = \"blood\""; #SQL query to fetch sample ID's matching tissue type
my $sth = $dbh -> prepare($sql);
$sth -> execute(); #executing SQL query
#my $rows = $sth -> dump_results(); 
while(my @row = $sth -> fetchrow_array()){ #looping through output of query by line
    push(@pureID, $row[0]); #storing ID's into array
}
column_extract(\@pureID, $dbh);

sub column_extract{ #subroutine used to extract matching sampleI ID's from raw genetpm1.txt file
    
    my (@ids) = @{$_[0]}; #ID's from initial query
    my ($dbh) = $_[1];
    
    my $dir = "Purified GTEx tpm1 data";
    mkdir($dir);
    opendir(DIR, $dir) or die "Couldn't open $dir\n"; #opening new directory
    open OUTFILE, ">", "$dir/Pure_Data_test.txt" or die "Couldn't open file\n"; #creating new file in directory for output
    
    open DATA, "/Users/Philip/Documents/BTMN 670/GTEx files/GTEx_genetpm1.txt", or die "Couldn't open gene tpm1 file\n"; #opening raw genetpm1.txt
    <DATA>; #skipping line 1
    <DATA>; #skipping line 2
    my @header = split/\t/, <DATA>; #splitting line 3 header line (Sample ID's) of file into array
    my %column_index; #initializing hash table
    @column_index{@header} = 0..$#header; #creating arrays equal to number of sample ID's 
    
    @ids = grep exists $column_index{$_}, @ids; #comparing id's from gtex_master_view query to column headers from file overriding @ids to only equal matched ID's
    
    print OUTFILE "Name\tDescription\t", join("\t", @ids, "\n"); #printing header to output file
    while(<DATA>){ #looping through file
        chomp(my @cells = split/\t/); #splitting rows of line into columns
        print OUTFILE $cells[0], "\t", $cells[1], "\t"; #printing gene id and name row
        print OUTFILE join ("\t", @cells [@column_index{@ids}], "\n"); #printing sample tpm1 values
    }
    close(DATA); 
    close(OUTFILE);
    close(DIR);
    header_parser($dbh);
}

sub header_parser{
    
    my ($dbh) = @_;
    
    my @fields;
    my $rawFields;
    my $counter = 0;
    my $dir = "Genetmp1_table";

    open (my $file, "<", "/Users/Philip/Documents/BTMN 670/Purified GTEx tpm1 data/Pure_Data_test.txt") or die $!; #opening newly created purified data file
    while (my $line = <$file>){ #looping through file
    
        chomp $line; 
        if($counter == 0){ # grabbing header line
            $rawFields = $line;
        }
        elsif($counter != 0){
            next;
        }
        $counter++;
    }
    close($file);
    @fields = split /\s+/, $rawFields; #splitting header

    my (@pure_view_ids, @queryStatement);
    my ($nameString, $descriptionString);
 
    for(my $i = 0; $i < @fields; $i++){
        if($i == 1000){ #stopping loop at 1000
            last;
        }
        elsif($fields[$i] =~ /Name/){
            my $input = $fields[$i]." varchar(100),"; #creating create table statement substring
            push(@queryStatement, $input); #pushing to array
        }
        elsif($fields[$i] =~ /Description/){ 
            my $input = $fields[$i]." varchar(100),"; #creating create table statement substring
            push(@queryStatement, $input); #pushing to array
        }
        elsif($i == 999){
            my $input = "\`".$fields[$i]."\` "."float(11)"; #creating create table statement substring removing comma for sql formatting
            push(@queryStatement, $input); #pushing to array
            push(@pure_view_ids, $fields[$i]); #pushing unformatted sample id to array
        }
        else{
            my $input = "\`".$fields[$i]."\` "."float(11),"; #creating create table statement substring
            push(@queryStatement, $input); #pushing to array
            push(@pure_view_ids, $fields[$i]); #pushing unformatted sample id to array
        }
    }
    
    my $tableInput = join(" ", @queryStatement); #joining all elements of array for SQL query string
    my $finalQuery = "create table genetpm1(".$tableInput.");"; #SQL create table statement
    my $sql = "$finalQuery";
    my $sth = $dbh -> prepare($sql);
    $sth -> execute(); 
    
    #loading data from local drive to newly created table on RDS cloud 
    my $sql2 = "load data local infile \'/Users/Philip/Documents/BTMN 670/Purified GTEx tpm1 data/Pure_Data_test.txt\' into table genetpm1_test fields terminated by \"\t\" lines terminated by \"\n\" ignore 1 lines";
    my $sth2 = $dbh -> prepare($sql2);
    $sth2 -> execute();
    
    purified_view(\@pure_view_ids, $dbh);
}

sub purified_view{ #subroutine to create updated view of GTEx_master_view with accurate sample ID's
    my (@pureID) = @{$_[0]};
    my ($dbh) = $_[1];
    my $view_input = join ' or sampid = ', map{qq/"$_"/}@pureID; #formatting SQL statement string
    my $sql = "create or replace view pure_gtex_master_view as select * from gtex_master_view where sampid = $view_input"; #sql statement
    my $sth = $dbh -> prepare($sql);
    $sth -> execute(); #statement execution
}

