-- Bulk insert new sites (35 total)
-- Run this in Supabase SQL editor to complete provisioning
-- Tier enum values: 'flagship' | 'daily' | 'vertical'

-- Daily wisdom sites (10) - use 'daily' tier
INSERT INTO sites (domain, name, tier, site_key, publish_threshold, auto_publish_enabled, logo_url, favicon_url, seo_defaults)
VALUES
  ('dailymarcus.com', 'Daily Marcus', 'daily', 'dailymarcus', 7.5, false, '/sites/dailymarcus/logo.svg', '/sites/dailymarcus/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('dailyseneca.com', 'Daily Seneca', 'daily', 'dailyseneca', 7.5, false, '/sites/dailyseneca/logo.svg', '/sites/dailyseneca/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('dailyaquinas.com', 'Daily Aquinas', 'daily', 'dailyaquinas', 7.5, false, '/sites/dailyaquinas/logo.svg', '/sites/dailyaquinas/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('dailyaristotle.com', 'Daily Aristotle', 'daily', 'dailyaristotle', 7.5, false, '/sites/dailyaristotle/logo.svg', '/sites/dailyaristotle/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('dailybible.info', 'Daily Bible', 'daily', 'dailybible', 7.5, false, '/sites/dailybible/logo.svg', '/sites/dailybible/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('dailylatin.info', 'Daily Latin', 'daily', 'dailylatin', 7.5, false, '/sites/dailylatin/logo.svg', '/sites/dailylatin/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('dailyproverbs.info', 'Daily Proverbs', 'daily', 'dailyproverbs', 7.5, false, '/sites/dailyproverbs/logo.svg', '/sites/dailyproverbs/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('dailytao.info', 'Daily Tao', 'daily', 'dailytao', 7.5, false, '/sites/dailytao/logo.svg', '/sites/dailytao/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('dailymasterpiece.art', 'Daily Masterpiece', 'daily', 'dailymasterpiece', 7.5, false, '/sites/dailymasterpiece/logo.svg', '/sites/dailymasterpiece/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('betterquotidian.com', 'Better Quotidian', 'daily', 'betterquotidian', 7.5, false, '/sites/betterquotidian/logo.svg', '/sites/betterquotidian/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}')
ON CONFLICT (domain) DO UPDATE SET
  name = EXCLUDED.name,
  tier = EXCLUDED.tier,
  site_key = EXCLUDED.site_key,
  publish_threshold = EXCLUDED.publish_threshold,
  logo_url = EXCLUDED.logo_url,
  favicon_url = EXCLUDED.favicon_url,
  seo_defaults = EXCLUDED.seo_defaults;

-- Health longevity sites (22) - use 'vertical' tier
INSERT INTO sites (domain, name, tier, site_key, publish_threshold, auto_publish_enabled, logo_url, favicon_url, seo_defaults)
VALUES
  ('sleepdepthlab.com', 'Sleep Depth Lab', 'vertical', 'sleepdepthlab', 8.0, false, '/sites/sleepdepthlab/logo.svg', '/sites/sleepdepthlab/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('sleeplab.info', 'Sleep Lab', 'vertical', 'sleeplab', 8.0, false, '/sites/sleeplab/logo.svg', '/sites/sleeplab/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('nootropiclab.com', 'Nootropic Lab', 'vertical', 'nootropiclab', 8.0, false, '/sites/nootropiclab/logo.svg', '/sites/nootropiclab/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('nootropics.info', 'Nootropics', 'vertical', 'nootropics', 8.0, false, '/sites/nootropics/logo.svg', '/sites/nootropics/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('peptidehub.com', 'Peptide Hub', 'vertical', 'peptidehub', 8.0, false, '/sites/peptidehub/logo.svg', '/sites/peptidehub/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('gutprotocol.com', 'Gut Protocol', 'vertical', 'gutprotocol', 8.0, false, '/sites/gutprotocol/logo.svg', '/sites/gutprotocol/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('gihealth.info', 'GI Health', 'vertical', 'gihealth', 8.0, false, '/sites/gihealth/logo.svg', '/sites/gihealth/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('guthealth.info', 'Gut Health', 'vertical', 'guthealth', 8.0, false, '/sites/guthealth/logo.svg', '/sites/guthealth/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('hormoneclearinghouse.com', 'Hormone Clearinghouse', 'vertical', 'hormoneclearinghouse', 8.0, false, '/sites/hormoneclearinghouse/logo.svg', '/sites/hormoneclearinghouse/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('hormones.info', 'Hormones', 'vertical', 'hormones', 8.0, false, '/sites/hormones/logo.svg', '/sites/hormones/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('herhormones.com', 'Her Hormones', 'vertical', 'herhormones', 8.0, false, '/sites/herhormones/logo.svg', '/sites/herhormones/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('menshormones.com', 'Mens Hormones', 'vertical', 'menshormones', 8.0, false, '/sites/menshormones/logo.svg', '/sites/menshormones/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('menshealthprotocol.com', 'Mens Health Protocol', 'vertical', 'menshealthprotocol', 8.0, false, '/sites/menshealthprotocol/logo.svg', '/sites/menshealthprotocol/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('humanlongevity.info', 'Human Longevity', 'vertical', 'humanlongevity', 8.0, false, '/sites/humanlongevity/logo.svg', '/sites/humanlongevity/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('longevitystack.com', 'Longevity Stack', 'vertical', 'longevitystack', 8.0, false, '/sites/longevitystack/logo.svg', '/sites/longevitystack/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('metabolichealth.info', 'Metabolic Health', 'vertical', 'metabolichealth', 8.0, false, '/sites/metabolichealth/logo.svg', '/sites/metabolichealth/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('metabolicshift.com', 'Metabolic Shift', 'vertical', 'metabolicshift', 8.0, false, '/sites/metabolicshift/logo.svg', '/sites/metabolicshift/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('metabolic.info', 'Metabolic', 'vertical', 'metabolic', 8.0, false, '/sites/metabolic/logo.svg', '/sites/metabolic/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('labpanel.info', 'Lab Panel', 'vertical', 'labpanel', 8.0, false, '/sites/labpanel/logo.svg', '/sites/labpanel/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('lamphilllabs.com', 'LampHill Labs', 'vertical', 'lamphilllabs', 8.0, false, '/sites/lamphilllabs/logo.svg', '/sites/lamphilllabs/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('coldtherapylab.com', 'Cold Therapy Lab', 'vertical', 'coldtherapylab', 8.0, false, '/sites/coldtherapylab/logo.svg', '/sites/coldtherapylab/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('carnivore.info', 'Carnivore', 'vertical', 'carnivore', 8.0, false, '/sites/carnivore/logo.svg', '/sites/carnivore/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}')
ON CONFLICT (domain) DO UPDATE SET
  name = EXCLUDED.name,
  tier = EXCLUDED.tier,
  site_key = EXCLUDED.site_key,
  publish_threshold = EXCLUDED.publish_threshold,
  logo_url = EXCLUDED.logo_url,
  favicon_url = EXCLUDED.favicon_url,
  seo_defaults = EXCLUDED.seo_defaults;

-- Other niche sites (3) - use 'vertical' tier
INSERT INTO sites (domain, name, tier, site_key, publish_threshold, auto_publish_enabled, logo_url, favicon_url, seo_defaults)
VALUES
  ('ripthroughtherange.com', 'Rip Through The Range', 'vertical', 'ripthroughtherange', 8.0, false, '/sites/ripthroughtherange/logo.svg', '/sites/ripthroughtherange/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('helloyou.life', 'Hello You', 'vertical', 'helloyou', 7.5, false, '/sites/helloyou/logo.svg', '/sites/helloyou/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}'),
  ('magpiediagnostics.com', 'Magpie Diagnostics', 'vertical', 'magpiediagnostics', 8.0, false, '/sites/magpiediagnostics/logo.svg', '/sites/magpiediagnostics/favicon.svg', '{"title_pattern": "{post_title} | {site_name}", "meta_description_pattern": "{excerpt}"}')
ON CONFLICT (domain) DO UPDATE SET
  name = EXCLUDED.name,
  tier = EXCLUDED.tier,
  site_key = EXCLUDED.site_key,
  publish_threshold = EXCLUDED.publish_threshold,
  logo_url = EXCLUDED.logo_url,
  favicon_url = EXCLUDED.favicon_url,
  seo_defaults = EXCLUDED.seo_defaults;

-- Verify insertion
SELECT site_key, domain, name, tier FROM sites ORDER BY created_at DESC LIMIT 40;
