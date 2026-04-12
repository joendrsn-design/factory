"""Final integration test — all 13 sites through full pipeline."""
import os, tempfile, json
from artifacts import *
from site_loader import SiteLoader
from topic_generator import TopicGenerator
from research import ResearchModule
from planning import PlanningModule
from write import WriteModule, get_voice_profile
from qa import QAModule, build_scoring_prompt
from deposit import DepositEngine, build_obsidian_frontmatter
from orchestrator import RealtimePipeline, pipeline_status

loader = SiteLoader('config/sites')
sites = loader.list_sites()
print(f'✅ Loaded {len(sites)} site configs: {sorted(sites)}')
assert len(sites) == 13, f'Expected 13 sites, got {len(sites)}'

# Initialize all modules
gen = TopicGenerator(config_dir='config/sites')
res = ResearchModule(config_dir='config/sites')
plan = PlanningModule(config_dir='config/sites')
write = WriteModule(config_dir='config/sites')
qa = QAModule(config_dir='config/sites')
deposit = DepositEngine(config_dir='config/sites')
print('✅ All 6 modules initialized')

run_id = new_run_id()
errors = []

for site_id in sorted(sites):
    try:
        ctx = loader.load(site_id)
        
        # Verify required fields
        assert ctx.site_id == site_id
        assert ctx.site_name
        assert ctx.niche
        assert ctx.voice.get('tone')
        assert ctx.audience.get('profile')
        
        # Verify article types
        types = ctx.get_enabled_article_types()
        assert len(types) > 0, f'{site_id}: no enabled article types'
        
        for at in types:
            assert at.get('type_id'), f'{site_id}: article type missing type_id'
            assert at.get('word_count_min', 0) > 0, f'{site_id}/{at["type_id"]}: no word_count_min'
        
        # Verify voice profile selection
        voice = get_voice_profile(ctx)
        assert len(voice) > 50, f'{site_id}: voice profile too short ({len(voice)} chars)'
        
        # Verify QA scoring adapts
        score_prompt = build_scoring_prompt(ctx, types[0])
        assert 'voice_fidelity' in score_prompt
        
        # Citation check: medical sites require citations
        if ctx.niche in ('medical-clinical', 'medical-laboratory', 'health-longevity'):
            citation_types = [t for t in types if t.get('citation_required')]
            assert len(citation_types) > 0, f'{site_id}: medical site should have citation-required types'
        
        # Verify output config
        assert ctx.output.get('obsidian_folder'), f'{site_id}: no obsidian_folder'
        
        # Verify full chain validation
        art_id = new_article_id()
        
        # Topic → Research
        topic_meta = topic_metadata(run_id, art_id, site_id, types[0]['type_id'],
                                    f'Test topic for {site_id}', ['test'], 'Test angle')
        v, e = res.validate_input(topic_meta, f'Test topic body for {site_id}')
        assert v, f'{site_id} Research input: {e}'
        
        # Research → Planning
        res_meta = research_metadata(run_id, art_id, site_id, types[0]['type_id'],
                                     f'Test topic for {site_id}', 'moderate', 2,
                                     ['Finding 1', 'Finding 2'], [])
        v, e = plan.validate_input(res_meta, 'Research body with findings and evidence.')
        assert v, f'{site_id} Planning input: {e}'
        
        # Planning → Write
        wc = types[0].get('word_count_min', 800)
        plan_meta = plan_metadata(run_id, art_id, site_id, types[0]['type_id'],
                                  f'Test for {site_id}', 'Test Title', 'test-slug',
                                  wc, '', '', ['test'], [], 3)
        plan_body = f'Plan body for {site_id} with introduction section covering the topic. Evidence section with data. Application section with practical guidance. Conclusion section wrapping up.'
        v, e = write.validate_input(plan_meta, plan_body)
        assert v, f'{site_id} Write input: {e}'
        
        # Write → QA
        art_meta = article_metadata(run_id, art_id, site_id, types[0]['type_id'],
                                    'Test Title', 'test-slug', wc, '', '', ['test'])
        art_meta['topic'] = f'Test topic for {site_id}'
        art_body = f'# Test Title\n\nSubstantive article content for {site_id} with enough words to pass the minimum character threshold for QA validation.'
        v, e = qa.validate_input(art_meta, art_body)
        assert v, f'{site_id} QA input: {e}'
        
        # QA → Deposit frontmatter
        qa_meta = qa_metadata(run_id, art_id, site_id, types[0]['type_id'],
                              'PUBLISH', 8.0, {}, 'Good', '', 0)
        qa_meta.update({'title': 'Test', 'slug': 'test', 'word_count': wc, 'tags': ['test']})
        fm = build_obsidian_frontmatter(qa_meta, ctx)
        assert fm['draft'] == False
        assert fm['_factory']['site_id'] == site_id
        
        type_ids = [t['type_id'] for t in types]
        print(f'  ✅ {site_id:30s} | {ctx.tier:10s} | {ctx.niche:25s} | types: {type_ids}')
        
    except Exception as ex:
        errors.append(f'{site_id}: {ex}')
        print(f'  ❌ {site_id}: {ex}')

print()
if errors:
    print(f'❌ {len(errors)} ERRORS:')
    for e in errors:
        print(f'  {e}')
else:
    print(f'🏆 ALL 13 SITES PASS FULL PIPELINE VALIDATION')
    
# Verify orchestrator can see all sites
pipeline = RealtimePipeline(config_dir='config/sites')
assert len(pipeline.loader.list_sites()) == 13
print(f'✅ Orchestrator sees all 13 sites')

# Site tier breakdown
tiers = {}
for sid in sites:
    ctx = loader.load(sid)
    tiers.setdefault(ctx.tier, []).append(sid)
print(f'\nSite breakdown:')
for tier, site_list in sorted(tiers.items()):
    print(f'  {tier}: {len(site_list)} sites')
