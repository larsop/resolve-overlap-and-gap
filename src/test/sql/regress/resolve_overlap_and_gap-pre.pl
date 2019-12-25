#!/usr/bin/perl
use File::Copy;
use File::Spec::Functions;

$FILE_NAME_PRE='resolve_overlap_and_gap-pre.sql';
print "\n Output file is $FILE_NAME_PRE \n";

open($fh_out, ">", $FILE_NAME_PRE);

# get funtion defs for overlap gab 
for my $file (glob '../../../main/sql/func*') {
	copy_file_into($file,$fh_out);
}

# get def for content based grid, 
# TODO find another way to pick up this from https://github.com/larsop/content_balanced_grid
copy_file_into('resolve_overlap_gap-pre-cbg-def.sql',$fh_out);
print "use the resolve_overlap_gap-pre-cbg-def.sql \n";

# get code for overlap and gap
# TODO find another way to pick up this from https://github.com/larsop/find-overlap-and-gap
copy_file_into('resolve_overlap_gap-pre-find-overlap-gap-def.sql',$fh_out);

# get execute paralell TODO find another way to pick up this from https://github.com/larsop/postgres_execute_parallel
copy_file_into('resolve_overlap_gap-pre-execute-par.sql',$fh_out);
print "use the find_overlap_gap-pre-execute-par.sql \n";

#copy input file
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


 