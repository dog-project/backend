CREATE TYPE primaries_race AS ENUM (
	'Black',
	'White',
	'Hispanic/Latinx',
	'Asian',
	'Native Am.',
	'Hawaiian/Pacific Isl.',
	'Middle Eastern/North African',
	'Other/Unknown',
	'Prefer not to say'
);

CREATE TYPE primaries_gender AS ENUM (
	'Man',
	'Woman',
	'Nonbinary',
	'Other',
	'Prefer not to say'
);

CREATE TYPE primaries_education_level AS ENUM (
	'Some high school',
	'HS diploma/GED',
	'Some College',
	'College or Beyond',
	'Prefer not to say',
);

CREATE TYPE primaries_age AS ENUM (
	'18-24',
	'25-44',
	'45-65',
	'65+',
	'Prefer not to say'
);

CREATE TYPE primaries_political_party AS ENUM (
	'Democrat',
	'Republican',
	'Independent',
	'Other',
	'Prefer not to say'
);

CREATE TYPE states AS ENUM (
	'AL',
	'AK',
	'AZ',
	'AR',
	'CA',
	'CO',
	'CT',
	'DE',
	'FL',
	'GA',
	'HI',
	'ID',
	'IL',
	'IN',
	'IA',
	'KS',
	'KY',
	'LA',
	'ME',
	'MD',
	'MA',
	'MI',
	'MN',
	'MS',
	'MO',
	'MT',
	'NE',
	'NV',
	'NH',
	'NJ',
	'NM',
	'NY',
	'NC',
	'ND',
	'OH',
	'OK',
	'OR',
	'PA',
	'RI',
	'SC',
	'SD',
	'TN',
	'TX',
	'UT',
	'VT',
	'VA',
	'WA',
	'WV',
	'WI',
	'WY'
);

CREATE TABLE voters
(
	id				INT				PRIMARY KEY		AUTO_INCREMENT,
    state 			text		NOT NULL,
    age_above_18	BOOLEAN 		NOT NULL 		DEFAULT FALSE,
    eligible 		BOOLEAN			NOT NULL 		DEFAULT FALSE,
    race			primaries_race		DEFAULT NULL,
    gender			primaries_gender		DEFAULT NULL,
    education 		primaries_education_level		DEFAULT NULL,
    age				primaries_age		DEFAULT NULL, 
    party			primaries_political_party		DEFAULT NULL,
    lgbtq			BOOLEAN			DEFAULT NULL
);

CREATE TABLE ballot
(
	id				INT 			PRIMARY KEY 	AUTO_INCREMENT,
    voter			INT 			NOT NULL,
    top_candidate 	text 	NOT NULL,
    tier1			text[],
    tier2			text[],
    tier3			text[],
    tier4			text[],
    tier5			text[],
    tier6			text[],
    unranked			text[]
)


