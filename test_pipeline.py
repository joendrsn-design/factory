"""Full pipeline integration test — Topic Generator through Deposit."""
import os, tempfile, json
from artifacts import *
from site_loader import SiteLoader
from topic_generator import TopicGenerator, PublishingHistory
from deposit import DepositEngine, build_obsidian_frontmatter, build_filename, clean_article_body

print('✅ All imports clean')

loader = SiteLoader('config/sites')
lamphill = loader.load('lamphill')
marcus = loader.load('daily-marcus-aurelius')
run_id = new_run_id()
tmpdir = tempfile.mkdtemp()

# ═══ TOPIC GENERATOR ═══

# 1: Publishing history
topics_dir = os.path.join(tmpdir, 'pipeline', 'topics')
os.makedirs(topics_dir, exist_ok=True)
for t in ['Magnesium Threonate Sleep', 'Vitamin D Immunity']:
    save_artifact(topic_metadata(run_id, new_article_id(), 'lamphill', 'deep_dive', t, [], ''),
                  f'Topic about {t}', topics_dir)
history = PublishingHistory(pipeline_dirs=[topics_dir])
existing = history.get_existing_topics('lamphill')
assert 'magnesium threonate sleep' in existing
print('✅ 1: Publishing history scans for existing topics')

# 2: Init
gen = TopicGenerator(config_dir='config/sites')
assert 'lamphill' in gen.loader.list_sites()
print('✅ 2: TopicGenerator loads all site configs')

# 3: Frequency distribution
article_types = lamphill.get_enabled_article_types()
dist = gen._distribute_by_frequency(article_types, 10)
assert sum(c for _, c in dist) == 10
print(f'✅ 3: Frequency distribution: {[(at["type_id"], c) for at, c in dist]}')

# 4: Topic parsing
mock = json.dumps([
    {'topic': 'Creatine for Cognitive Function', 'keywords': ['creatine brain'], 'angle': 'Beyond muscle', 'notes': 'Meta-analysis available'},
    {'topic': 'Omega-3 DHA vs EPA for Heart Health', 'keywords': ['omega 3'], 'angle': 'Comparison', 'notes': 'Dose-response data'},
    {'topic': 'Berberine as Metformin Alternative', 'keywords': ['berberine'], 'angle': 'Physician analysis', 'notes': 'Balanced take'},
])
parsed = gen._parse_topics(mock, lamphill, article_types[0], run_id)
assert len(parsed) == 3
assert parsed[0][0]['module'] == 'topic_generator'
assert parsed[0][0]['site_id'] == 'lamphill'
print(f'✅ 4: Parsed {len(parsed)} topics from mock response')

# 5: Save + roundtrip
out_topics = os.path.join(tmpdir, 'output_topics')
gen.save_topics(parsed, out_topics)
loaded = load_artifacts_from_dir(out_topics, module_filter='topic_generator')
assert len(loaded) == 3
print('✅ 5: Topics saved and reloaded as artifacts')

# ═══ DEPOSIT MODULE ═══

# 6: Obsidian frontmatter
qa_pub = qa_metadata(run_id, new_article_id(), 'lamphill', 'deep_dive', 'PUBLISH', 8.5, {'voice': 9}, 'Great', '', 0)
qa_pub.update({'title': 'MgT Sleep Evidence', 'slug': 'mgt-sleep-evidence', 'seo_title': 'MgT Guide',
               'meta_description': 'Evidence review.', 'tags': ['magnesium', 'sleep'], 'word_count': 2400})
fm = build_obsidian_frontmatter(qa_pub, lamphill)
assert fm['title'] == 'MgT Sleep Evidence'
assert fm['draft'] == False
assert fm['qa_score'] == 8.5
assert '_factory' in fm
print('✅ 6: Obsidian frontmatter (publish-ready)')

# 7: Filename
fname = build_filename(qa_pub, lamphill)
assert fname.endswith('.md')
print(f'✅ 7: Filename: {fname}')

# 8: Body cleaning
dirty = "# Title\n\nGood.\n\n\n---\n\n" + "```" + "json\n[]\n" + "```" + "\n\n\n\n\nExtra lines."
clean = clean_article_body(dirty)
assert "json" not in clean
assert "\n\n\n\n" not in clean
print('✅ 8: Body cleaned')

# 9: Deposit flow (dry run)
qa_dir = os.path.join(tmpdir, 'pipeline', 'qa')
os.makedirs(qa_dir, exist_ok=True)
article_body = '# MgT for Sleep\n\nEvidence about magnesium threonate [1].\n\n## References\n\n1. Study.'
pub = qa_metadata(run_id, new_article_id(), 'lamphill', 'deep_dive', 'PUBLISH', 8.5, {}, 'Good', '', 0)
pub.update({'title': 'MgT Sleep Guide', 'slug': 'mgt-sleep-guide', 'word_count': 150, 'tags': ['mg']})
save_artifact(pub, article_body, qa_dir)

rw = qa_metadata(run_id, new_article_id(), 'lamphill', 'deep_dive', 'REWRITE', 5.8, {}, 'Needs work', 'Fix voice', 0)
rw.update({'title': 'Weak', 'slug': 'weak'})
save_artifact(rw, 'Weak', qa_dir)

kl = qa_metadata(run_id, new_article_id(), 'lamphill', 'deep_dive', 'KILL', 3.0, {}, 'Bad', '', 0)
kl.update({'title': 'Dead', 'slug': 'dead'})
save_artifact(kl, 'Bad', qa_dir)

engine = DepositEngine(config_dir='config/sites')
summary = engine.deposit(input_dir=qa_dir, dry_run=True)
assert len(summary['published']) == 1 and len(summary['skipped_rewrite']) == 1 and len(summary['skipped_kill']) == 1
print('✅ 9: Dry run: 1 publish, 1 rewrite, 1 kill')

# 10: Real deposit
summary_real = engine.deposit(input_dir=qa_dir)
assert len(summary_real['published']) == 1
output_path = summary_real['published'][0]['output_path']
assert os.path.exists(output_path)
with open(output_path) as f:
    content = f.read()
assert 'draft: false' in content
assert 'qa_score: 8.5' in content
print(f'✅ 10: Deposited to: {output_path}')

# 11: Report
report = engine.generate_report(summary_real)
assert '**Published:** 1' in report and 'MgT Sleep Guide' in report
print(f'✅ 11: Report generated ({len(report)} chars)')

# ═══ FULL CHAIN ═══

# 12: Every module validates its neighbor's output
from research import ResearchModule
from planning import PlanningModule
from write import WriteModule
from qa import QAModule

res = ResearchModule(config_dir='config/sites')
assert res.validate_input(parsed[0][0], parsed[0][1])[0]  # Topic → Research

plan = PlanningModule(config_dir='config/sites')
r_meta = research_metadata(run_id, new_article_id(), 'lamphill', 'deep_dive', 'T', 'deep', 2, ['A','B'], [])
assert plan.validate_input(r_meta, 'Research body with evidence.')[0]  # Research → Planning

write = WriteModule(config_dir='config/sites')
p_meta = plan_metadata(run_id, new_article_id(), 'lamphill', 'deep_dive', 'T', 'Title', 'slug', 2500, '', '', [], [], 6)
assert write.validate_input(p_meta, 'Plan body with intro section evidence section mechanism section dosing section and conclusion section fully described.')[0]

qa_mod = QAModule(config_dir='config/sites')
a_meta = article_metadata(run_id, new_article_id(), 'lamphill', 'deep_dive', 'T', 's', 240, '', '', [])
a_meta['topic'] = 'Test'
assert qa_mod.validate_input(a_meta, '# Title\n\nArticle with evidence about the topic and clinical data supporting claims [1].\n\n## References\n\n1. Source.')[0]

print('✅ 12: FULL CHAIN: TopicGen → Research → Planning → Write → QA → Deposit ✓')
print()
print('🏆 ALL 12 TESTS PASS — Pipeline complete')
