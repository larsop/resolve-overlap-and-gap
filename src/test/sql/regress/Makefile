
check: loader #00-regress
	POSTGIS_REGRESS_DB=nibio_reg ./run_test.pl -v --spatial_ref_sys --extension --topology $(RUNTESTFLAGS) ./resolve_overlap_and_gap

loader:
	mkdir -p ../loader; \
	touch ../loader/shp2pgsql
	chmod +x ../loader/shp2pgsql
	touch ../loader/pgsql2shp
	chmod +x ../loader/pgsql2shp

