#!/usr/bin/perl
use File::Copy;
use File::Spec::Functions;

# if submodules exists use code from them and update resolve_overlap_and_gap-pre-def.sql or else just use the existing one
if (-d "../../../../submodule") {

	open($fh_out, ">", 'resolve_overlap_and_gap-pre-def.sql');
	
	# git submodule add --force https://github.com/larsop/content_balanced_grid submodule/content_balanced_grid
	# get functions for content_balanced_grid
	for my $file (glob '../../../../submodule/content_balanced_grid/func_grid/func*') {
		copy_file_into($file,$fh_out);
	}
	
	# git submodule add --force https://github.com/larsop/postgres_execute_parallel submodule/postgres_execute_parallel
	# get functions for postgres_execute_parallel
	for my $file (glob '../../../../submodule/postgres_execute_parallel/src/main/sql/func*') {
		copy_file_into($file,$fh_out);
	}

	# git submodule add --force https://github.com/larsop/find-overlap-and-gap submodule/find-overlap-and-gap
	# get functions for find-overlap-and-gap
	for my $file (glob '../../../../submodule/find-overlap-and-gap/src/main/sql/func*') {
		copy_file_into($file,$fh_out);
	}

	close($fh_out);	 

}

# Get code from this repo which always exits 

$FILE_NAME_PRE='resolve_overlap_and_gap-pre.sql';
print "\n Output file is $FILE_NAME_PRE \n";

open($fh_out, ">", $FILE_NAME_PRE);

# get funtion defs for overlap gab 
for my $file (glob '../../../main/sql/func*') {
	copy_file_into($file,$fh_out);
}

#copy input file
copy_file_into('resolve_overlap_and_gap-pre-def.sql',$fh_out);
copy_file_into('overlap_gap_input_t1.sql',$fh_out);


close($fh_out);	 

sub copy_file_into() { 
	my ($v1, $v2) = @_;
	open(my $fh, '<',$v1);
	while (my $row = <$fh>) {
	  print $v2 "$row";
	}
	close($fh);	 
    
}


 