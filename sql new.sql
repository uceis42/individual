drop table if exists public.merged cascade;


   
create table public.merged (
    RegionCountry    varchar(256),
    SecondTierAuthority                  varchar(256),
    LocalAuthority                  varchar(256) ,
    Code                varchar(256),
    Year         varchar(256),
    IndustryElectricity	             float,
    IndustryGas          float,
    IndustryOtherFuels           float,
    LargeIndustrialInstallations                  float,
    Agriculture              float,
    IndustryTotal     float,
    LocalAuthorityDistractName    varchar(256),
    Operator                  varchar(256),
    Site                  varchar(256),
    Postcode                varchar(256),
    Reference         varchar(256),
    SubstanceName             varchar(256),
    year2019                 float,
    label                 varchar(256)
    );