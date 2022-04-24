drop table if exists public.companyList cascade;
drop table if exists public.companyDetail cascade;
drop table if exists public.pollutionInventory cascade;
drop table if exists public.CO2emissions cascade;

create table public.companyList (
    id           serial primary key,
    companyNameLetter    varchar(256),
    companyName   varchar(256),
    companyNumber              varchar(256) unique,
    status               varchar(256),
    link                 varchar(256)
);

create table public.companyDetail (
    companyNameLetter    varchar(256) , 
    companyName                  varchar(256),
    companyNumber                  varchar(256) REFERENCES public.companyList (companyNumber),
    address                varchar(256),
    companyType         varchar(256),
    incorporated             varchar(256),
    industryCode          varchar(256),
    postCode             varchar(256),
    latitude              float,
    longitude              float
   
    );
    
create table public.pollutionInventory (
    LocalAuthorityDistractName    varchar(256),
    Operator                  varchar(256),
    Site                  varchar(256),
    Postcode                varchar(256),
    Reference         varchar(256),
    SubstanceName             varchar(256),
    year2005          float,
    year2006          float,
    year2007          float,
    year2008          float,
    year2009          float,
    year2010          float,
    year2011          float,
    year2012          float,
    year2013          float,
    year2014          float,
    year2015          float,
    year2016          float,
    year2017          float,
    year2018          float,
    year2019          float   
    );
    
   
create table public.CO2emissions (
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
    IndustryTotal     float 
    );