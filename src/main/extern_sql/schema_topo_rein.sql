-- TODO remove this code and make it generic
-- create schema for topo_rein data, tables, ....

CREATE SCHEMA topo_rein;

-- give puclic access
GRANT USAGE ON SCHEMA topo_rein TO public;

-- This function is used to create indexes
CREATE OR REPLACE FUNCTION topo_rein.get_relation_id (geo TopoGeometry)
  RETURNS integer
  AS $$
DECLARE
  relation_id integer;
BEGIN
  relation_id := (geo).id;
  RETURN relation_id;
END;
$$
LANGUAGE plpgsql
IMMUTABLE;

COMMENT ON FUNCTION topo_rein.get_relation_id (TopoGeometry) IS 'Return the id used to find the row in the relation for polygons). Needed to create function based indexs.';

-- A composite type to hold sosi kopi_data
CREATE TYPE topo_rein.sosi_kopidata AS (
  omradeid smallint, originaldatavert Varchar ( 50), kopidato DATE);

-- A composite type to hold sosi registreringsversjon
CREATE TYPE topo_rein.sosi_registreringsversjon AS (
  produkt varchar, versjon varchar
);

-- A composite type to hold sosi kvalitet
-- beskrivelse av kvaliteten på stedfestingen

CREATE TYPE topo_rein.sosi_kvalitet AS (
  -- metode for måling i grunnriss (x,y), og høyde (z) når metoden er den samme som ved måling i grunnriss
  -- TODO Hentes fra kode tabell eller bruke en constraint ???

  maalemetode smallint,
  -- punktstandardavviket i grunnriss for punkter samt tverravvik for linjer
  -- Merknad: Oppgitt i cm

  noyaktighet integer,
  -- hvor godt den kartlagte detalj var synbar ved kartleggingen
  -- TODO Hentes fra kode tabell eller bruke en constraint ???

  synbarhet smallint
);

-- A composite type to hold sosi sosi felles egenskaper
CREATE TYPE topo_rein.sosi_felles_egenskaper AS (
  -- identifikasjondato når data ble registrert/observert/målt første gang, som utgangspunkt for første digitalisering
  -- Merknad:førsteDatafangstdato brukes hvis det er av interesse å forvalte informasjon om når en ble klar over objektet. Dette kan for eksempel gjelde datoen for første flybilde som var utgangspunkt for registrering i en database.
  -- lage regler for hvordan den skal brukes, kan i mange tilfeller arves
  -- henger sammen med UUID, ny UUID ny datofangst dato

  forstedatafangstdato DATE,
  -- Unik identifikasjon av et objekt, ivaretatt av den ansvarlige produsent/forvalter, som kan benyttes av eksterne applikasjoner som referanse til objektet.
  -- NOTE1 Denne eksterne objektidentifikasjonen må ikke forveksles med en tematisk objektidentifikasjon, slik som f.eks bygningsnummer.
  -- NOTE 2 Denne unike identifikatoren vil ikke endres i løpet av objektets levetid.
  -- TODO Test if we can use this as a unique id.

  identifikasjon varchar,
  -- bygd opp navnerom/lokalid/versjon
  -- navnerom: NO_LDIR_REINDRIFT_VAARBEITE
  -- versjon: 0
  -- lokalid:  rowid
  -- eks identifikasjon = "NO_LDIR_REINDRIFT_VAARBEITE 0 199999999"
  -- beskrivelse av kvaliteten på stedfestingen
  -- Merknad: Denne er identisk med ..KVALITET i tidligere versjoner av SOSI.

  kvalitet topo_rein.sosi_kvalitet,
  -- dato for siste endring på objektetdataene
  -- Merknad: Oppdateringsdato kan være forskjellig fra Datafangsdato ved at data som er registrert kan bufres en kortere eller lengre periode før disse legges inn i datasystemet (databasen).
  -- Definition: Date and time at which this version of the spatial object was inserted or changed in the spatial data set.

  oppdateringsdato DATE,
  -- referanse til opphavsmaterialet, kildematerialet, organisasjons/publiseringskilde
  -- Merknad: Kan også beskrive navn på person og årsak til oppdatering

  opphav Varchar ( 255),
  -- dato når dataene er fastslått å være i samsvar med virkeligheten
  -- Merknad: Verifiseringsdato er identisk med ..DATO i tidligere versjoner av SOSI	verifiseringsdato DATE
  -- lage regler for hvordan den skal brukes
  -- flybilde fra 2008 vil gi data 2008, må være input fra brukeren

  verifiseringsdato DATE,
  -- Hva gjør vi med disse verdiene som vi har brukt tidligere brukte  i AR5 ?
  -- Er vi sikre på at vi ikke trenger de
  -- datafangstdato DATE,
  -- Vet ikke om vi skal ha med den, må tenke litt
  -- Skal ikke være med hvis Knut og Ingvild ikke sier noe annet
  -- vil bli et produktspek til ???
  -- taes med ikke til slutt brukere

  informasjon Varchar(255)
  ARRAY,
  -- trengs ikke i følge Knut og Ingvild
  -- kopidata topo_rein.sosi_kopidata,
  -- trengs ikke i følge Knut og Ingvild
  -- prosess_historie VARCHAR(255) ARRAY,
  -- kan være forskjellige verdier ut fra når data ble lagt f.eks null verdier for nye attributter eldre enn 4.0
  -- bør være med

  registreringsversjon topo_rein.sosi_registreringsversjon);

-- this is type used extrac data from json
CREATE TYPE topo_rein.simple_sosi_felles_egenskaper AS (
  "fellesegenskaper.forstedatafangstdato" date, "fellesegenskaper.verifiseringsdato" date, "fellesegenskaper.oppdateringsdato" date, "fellesegenskaper.opphav" varchar,
  "fellesegenskaper.kvalitet.maalemetode" int, "fellesegenskaper.kvalitet.noyaktighet" int, "fellesegenskaper.kvalitet.synbarhet" smallint
);

-- A composite type to hold key value that will recoreded before a update
-- and compared after the update, used be sure no changes hapends out side
-- the area that should be updated
-- DROP TYPE topo_rein.closeto_values_type cascade;

CREATE TYPE topo_rein.closeto_values_type AS (
  -- line length that intersects reinflate
  closeto_length_reinflate_inter numeric,
  -- line count that intersects the edge
  closeto_count_edge_inter int,
  -- line count that intersetcs reinlinje
  closeto_count_reinlinje_inter int,
  -- used to check that attribute value close has not changed a close to
  artype_and_length_as_text text,
  -- used to check that the area is ok after update
  -- as we use today we do not remove any data we just add new polygins or change exiting
  -- the layer should always be covered

  envelope_area_inter numeric
);

-- TODO add more comments
COMMENT ON COLUMN topo_rein.sosi_felles_egenskaper.verifiseringsdato IS 'Sosi common meta attribute';

COMMENT ON COLUMN topo_rein.sosi_felles_egenskaper.opphav IS 'Sosi common meta attribute';

COMMENT ON COLUMN topo_rein.sosi_felles_egenskaper.informasjon IS 'Sosi common meta attribute';

