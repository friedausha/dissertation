-- =============================================================================
-- Seed data for development and testing
--
-- Inserts a small set of realistic synthetic incidents drawn from the kinds
-- of articles the pipeline will encounter, covering:
--   • Multiple crime types
--   • Multiple regions
--   • A range of data completeness (some NULLs to mirror real extraction)
--   • Date range spanning ~2 years for time-series query testing
--
-- These are SYNTHETIC records — not real incidents.
-- =============================================================================

-- Placeholder raw_articles rows that the incidents can reference
INSERT INTO raw_articles (url, title, domain, source_country, language,
                          seendate, query_label, full_text_status)
VALUES
    ('https://example-seed.com/1',
     'Myanmar scam compounds hold thousands of trafficking victims',
     'example-seed.com', 'US', 'English',
     '2024-03-10 08:00:00+00', 'online_scam_camps', 'success'),

    ('https://example-seed.com/2',
     'Pig-butchering romance scam networks dismantled in Philippines',
     'example-seed.com', 'US', 'English',
     '2024-06-22 10:30:00+00', 'pig_butchering', 'success'),

    ('https://example-seed.com/3',
     'Cambodia anti-trafficking crackdown rescues 200 workers',
     'example-seed.com', 'US', 'English',
     '2024-09-05 14:00:00+00', 'online_scam_camps', 'success'),

    ('https://example-seed.com/4',
     'Nigerian authorities arrest gang running West Africa scam hub',
     'example-seed.com', 'US', 'English',
     '2024-11-18 09:15:00+00', 'human_trafficking', 'success'),

    ('https://example-seed.com/5',
     'Crypto investment fraud: UK victims lose millions to pig-butchering ring',
     'example-seed.com', 'GB', 'English',
     '2024-12-03 11:00:00+00', 'pig_butchering', 'success'),

    ('https://example-seed.com/6',
     'Thailand police dismantle debt bondage network targeting migrants',
     'example-seed.com', 'TH', 'English',
     '2025-01-14 07:45:00+00', 'smuggling_debt', 'success'),

    ('https://example-seed.com/7',
     'Laos scam compound expands as trafficking crisis deepens in Mekong region',
     'example-seed.com', 'US', 'English',
     '2025-02-28 16:20:00+00', 'online_scam_camps', 'success'),

    ('https://example-seed.com/8',
     'South Africa sex trafficking ring broken up by police',
     'example-seed.com', 'ZA', 'English',
     '2025-03-11 09:00:00+00', 'human_trafficking', 'success'),

    ('https://example-seed.com/9',
     'Dubai romance scam network targeted victims across Europe',
     'example-seed.com', 'AE', 'English',
     '2025-04-07 13:30:00+00', 'pig_butchering', 'success'),

    ('https://example-seed.com/10',
     'Brazil forced labour crackdown uncovers sugar cane trafficking network',
     'example-seed.com', 'BR', 'English',
     '2025-05-20 10:00:00+00', 'human_trafficking', 'success')
ON CONFLICT (url) DO NOTHING;


-- Matching classification rows (all marked relevant)
INSERT INTO article_classifications (raw_article_id, is_relevant, reasoning, model_version)
SELECT id, TRUE,
       'Seed record — manually verified as a genuine scam-driven trafficking incident.',
       'seed'
FROM raw_articles
WHERE domain = 'example-seed.com'
ON CONFLICT (raw_article_id) DO NOTHING;


-- Structured incident records
INSERT INTO incidents (
    raw_article_id, article_url, article_title, article_domain,
    reported_date, incident_date,
    location_country, location_region,
    crime_type, victim_count, victim_nationality, perpetrator_nationality,
    summary, confidence, model_version
)
SELECT
    r.id,
    r.url,
    r.title,
    r.domain,
    vals.reported_date,
    vals.incident_date,
    vals.location_country,
    vals.location_region,
    vals.crime_type,
    vals.victim_count,
    vals.victim_nationality,
    vals.perpetrator_nationality,
    vals.summary,
    vals.confidence,
    'seed'
FROM raw_articles r
JOIN (VALUES
    ('https://example-seed.com/1',
     '2024-03-10'::DATE, '2024-02-01'::DATE,
     'Myanmar', 'Southeast Asia', 'scam_compound', 3000,
     'Multiple nationalities', 'Chinese', 'high',
     'An estimated 3,000 people from multiple countries were held in guarded compounds in Myanmar''s Shan State, forced to run online fraud operations targeting victims globally.'),

    ('https://example-seed.com/2',
     '2024-06-22'::DATE, NULL,
     'Philippines', 'Southeast Asia', 'pig_butchering', 45,
     'Filipino', 'Chinese', 'high',
     'Philippine authorities dismantled a pig-butchering ring that had defrauded overseas victims of an estimated $8 million through fake cryptocurrency investment platforms.'),

    ('https://example-seed.com/3',
     '2024-09-05'::DATE, '2024-08-20'::DATE,
     'Cambodia', 'Southeast Asia', 'scam_compound', 200,
     NULL, 'Cambodian', 'medium',
     'Cambodian police rescued 200 workers from a scam compound in Sihanoukville following an international tip-off; victims had been trafficked from Vietnam, China, and Indonesia.'),

    ('https://example-seed.com/4',
     '2024-11-18'::DATE, NULL,
     'Nigeria', 'West Africa', 'scam_compound', 30,
     'Nigerian', NULL, 'medium',
     'Lagos authorities arrested 12 suspects operating a fraud compound that used coerced workers to impersonate romantic partners and financial advisers targeting victims in the United States and Europe.'),

    ('https://example-seed.com/5',
     '2024-12-03'::DATE, NULL,
     'United Kingdom', 'Europe', 'pig_butchering', NULL,
     'British', NULL, 'low',
     'UK Action Fraud reported a surge in pig-butchering complaints in Q4 2024, with losses averaging £30,000 per victim; the syndicate is believed to operate out of Southeast Asia.'),

    ('https://example-seed.com/6',
     '2025-01-14'::DATE, '2025-01-10'::DATE,
     'Thailand', 'Southeast Asia', 'debt_bondage', 80,
     'Myanmar', 'Thai', 'high',
     'Thai police freed 80 Myanmar migrants held under debt bondage on fishing vessels in the Gulf of Thailand; workers had paid recruiters fees and were forced to work without pay to repay inflated debts.'),

    ('https://example-seed.com/7',
     '2025-02-28'::DATE, NULL,
     'Laos', 'Southeast Asia', 'scam_compound', 5000,
     'Multiple nationalities', 'Chinese', 'medium',
     'Reports indicate a major scam-compound operation in the Golden Triangle Special Economic Zone in Laos has grown to hold up to 5,000 trafficked workers from at least 10 countries.'),

    ('https://example-seed.com/8',
     '2025-03-11'::DATE, NULL,
     'South Africa', 'Southern Africa', 'sex_trafficking', 22,
     'South African', NULL, 'high',
     'South African police dismantled a sex trafficking network operating across Johannesburg and Cape Town, rescuing 22 women who had been recruited through fraudulent domestic-work advertisements.'),

    ('https://example-seed.com/9',
     '2025-04-07'::DATE, NULL,
     'United Arab Emirates', 'Middle East', 'pig_butchering', NULL,
     NULL, NULL, 'low',
     'Europol and UAE authorities jointly investigated a pig-butchering network using Dubai-registered entities to launder proceeds; European victims reported losses exceeding €15 million.'),

    ('https://example-seed.com/10',
     '2025-05-20'::DATE, '2025-04-01'::DATE,
     'Brazil', 'South America', 'forced_labour', 150,
     'Brazilian', 'Brazilian', 'high',
     'Brazilian labour inspectors freed 150 workers from a sugar cane farm in Mato Grosso do Sul operating under conditions analogous to slavery, with workers unable to leave due to debt and document confiscation.')
) AS vals(
    url, reported_date, incident_date,
    location_country, location_region, crime_type, victim_count,
    victim_nationality, perpetrator_nationality, confidence, summary
) ON vals.url = r.url
ON CONFLICT (raw_article_id) DO NOTHING;
