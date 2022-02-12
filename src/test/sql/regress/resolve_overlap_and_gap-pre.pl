#!/usr/bin/perl
use File::Copy;
use File::Spec::Functions;

# if submodules exists use code from them and update resolve_overlap_and_gap-pre-def.sql or else just use the existing one
if (-d "../../../../submodule") {

  my $tgt = 'resolve_overlap_and_gap-pre-def.sql';
	open($fh_out, ">", $tgt) or die ("Could not open $tgt for writing");
	
	# get type resolve for overlap gap 
    for my $file (glob '../../../main/sql/types*') {
	    copy_file_into($file,$fh_out) or die("Could not copy $file into $tgt");
    }

	
	
	# git submodule add --force https://github.com/larsop/content_balanced_grid submodule/content_balanced_grid
	# get functions for content_balanced_grid
	for my $file (glob '../../../../submodule/content_balanced_grid/func_grid/func*') {
		copy_file_into($file,$fh_out) or die("Could not copy $file into $tgt");
	}
	
	# git submodule add --force https://github.com/larsop/postgres_execute_parallel submodule/postgres_execute_parallel
	# get functions for postgres_execute_parallel
	for my $file (glob '../../../../submodule/postgres_execute_parallel/src/main/sql/func*') {
		copy_file_into($file,$fh_out) or die("Could not copy $file into $tgt");
	}

	# git submodule add --force https://github.com/larsop/find-overlap-and-gap submodule/find-overlap-and-gap
	# get functions for find-overlap-and-gap
	for my $file (glob '../../../../submodule/find-overlap-and-gap/src/main/sql/func*') {
		copy_file_into($file,$fh_out) or die("Could not copy $file into $tgt");
	}
	
	# Copy extra files needed by https://github.com/NibioOpenSource/pgtopo_update_sql.git
	my $file = '../../../../src/main/extern_sql/schema_topo_rein.sql';
	copy_file_into($file,$fh_out) or die("Could not copy $file into $tgt");
	

	# git submodule add --force https://github.com/NibioOpenSource/pgtopo_update_sql.git submodule/pgtopo_update_sql
	# get functions from pgtopo_update_sql repo
  for my $file (
'../../../../submodule/pgtopo_update_sql/src/main/sql/topo_update/schema_topo_update.sql',
'../../../../submodule/pgtopo_update_sql/src/main/sql/topo_update/schema_userdef_structures_02.sql',
'../../../../submodule/pgtopo_update_sql/src/main/sql/topo_update/function_02_handle_input_json_props.sql',
'../../../../submodule/pgtopo_update_sql/src/main/sql/topo_update/function_02_update_domain_surface_layer.sql',
'../../../../submodule/pgtopo_update_sql/src/main/sql/topo_update/function_01_make_input_meta_info.sql',
'../../../../submodule/pgtopo_update_sql/src/main/sql/topo_update/function_02_handle_input_json_props.sql',
'../../../../submodule/pgtopo_update_sql/src/main/sql/topo_update/function_01_get_geom_from_json.sql',
'../../../../submodule/pgtopo_update_sql/src/main/sql/topo_update/function_01_get_felles_egenskaper.sql'
  )
  {
	  copy_file_into($file, $fh_out) or die("Could not copy $file into $tgt");
  }
	
	close($fh_out);	 

}

# Create the complete file (resolve_overlap_and_gap-pre.sql) needed for the tests  
$tgt = 'resolve_overlap_and_gap-pre.sql';
open($fh_out, ">", $tgt);

#copy resolve_overlap_and_gap-pre-def.sql into final file (if no submodule the file from git is used)
$file = 'resolve_overlap_and_gap-pre-def.sql';
copy_file_into($file, $fh_out) or die("Could not copy $file into $tgt");

# get SQL code used by resolve for overlap gap from this repo
# (Maybe move this code to https://github.com/NibioOpenSource/pgtopo_update_sql.git)
for my $file (glob '../../../main/extern_pgtopo_update_sql/func*') {
	copy_file_into($file,$fh_out)
    or die("Could not copy $file into $tgt");
}
for my $file (glob '../../../main/extern_pgtopo_update_sql/view*') {
	copy_file_into($file,$fh_out);
}
for my $file (glob '../../../main/extern_pgtopo_update_sql/utils/func*') {
	copy_file_into($file,$fh_out);
}

# get SQL code resolve for overlap gap 
for my $file (glob '../../../main/sql/func*') {
	copy_file_into($file,$fh_out);
}

# make an install file 
close($fh_out);	 
open($fh_out_install, ">", 'resolve_overlap_and_gap-install.sql');
copy_file_into('resolve_overlap_and_gap-pre.sql', $fh_out_install) or die("Could not copy $file into $fh_out_install");
close($fh_out_install);	 

# open file to append rest of data 

open($fh_out, ">>", $tgt);
# get SQL code for test data needed by the tests 
copy_file_into('overlap_gap_input_t1.sql',$fh_out);

# get SQL code for test data needed by the tests 
copy_file_into('overlap_gap_input_t2.sql',$fh_out);

close($fh_out);	 

sub copy_file_into { 
	my ($v1, $v2) = @_;
	open(my $fh, '<',$v1);
	while (my $row = <$fh>) {
	  print $v2 "$row";
	}
	close($fh);	 
    
}


 
