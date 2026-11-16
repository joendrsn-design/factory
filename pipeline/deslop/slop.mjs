#!/usr/bin/env node
// slop.mjs — markdown-aware AI-slop detector (same rules as the Slop Detector web tool).
// Zero dependencies. Node 18+.
//
//   node slop.mjs <file-or-folder> [...]       report on .md/.mdx files (recursive)
//   --fix            apply the safe word/phrase swaps IN PLACE (run on a git-tracked folder)
//   --json           machine-readable output (for the Article Factory / QA gates)
//   --min <N>        exit code 1 if any file scores >= N (default 60)
//   --top <N>        findings shown per file in the text report (default 15, 0 = all)
//   --summary        one line per file, no findings
import fs from "node:fs";
import path from "node:path";

const CATS={
  word:{label:"Word choice"}, phrase:{label:"Stock phrases"}, construct:{label:"Constructions"},
  filler:{label:"Filler & hedges"}, grammar:{label:"Grammar & wordiness"}, rhythm:{label:"Rhythm"}
};
const B="\\b";
// Rule: id, cat, label, msg, re (source, flags gi), w (weight), fix: string | map | null, cap (capitalize after cut at sentence start)
const R=[];
function rule(cat,id,label,re,msg,fix,w,opts){R.push(Object.assign({cat,id,label,src:re,msg,fix:fix===undefined?null:fix,w:w==null?{word:2,phrase:3,construct:3,filler:1,grammar:1,rhythm:1}[cat]:w},opts||{}))}

// ---- Word choice ----
const words=[
 ["delve","delv(?:e|es|ed|ing)",{delve:"dig into",delves:"digs into",delved:"dug into",delving:"digging into"}],
 ["tapestry","tapestr(?:y|ies)",null],
 ["testament","testament",{testament:"proof"}],
 ["leverage","leverag(?:e|es|ed|ing)",{leverage:"use",leverages:"uses",leveraged:"used",leveraging:"using"}],
 ["utilize","utili[sz](?:e|es|ed|ing)",{utilize:"use",utilizes:"uses",utilized:"used",utilizing:"using",utilise:"use",utilises:"uses",utilised:"used",utilising:"using"}],
 ["robust","robust",{robust:"solid"}],
 ["seamless","seamless(?:ly)?",{seamless:"smooth",seamlessly:"smoothly"}],
 ["pivotal","pivotal",{pivotal:"key"}],
 ["crucial","crucial",{crucial:"key"}],
 ["landscape","landscapes?",null],
 ["realm","realms?",{realm:"field",realms:"fields"}],
 ["embark","embark(?:s|ed|ing)?",{embark:"start",embarks:"starts",embarked:"started",embarking:"starting"}],
 ["foster","foster(?:s|ed|ing)?",{foster:"build",fosters:"builds",fostered:"built",fostering:"building"}],
 ["bolster","bolster(?:s|ed|ing)?",{bolster:"strengthen",bolsters:"strengthens",bolstered:"strengthened",bolstering:"strengthening"}],
 ["underscore","underscor(?:e|es|ed|ing)",{underscore:"show",underscores:"shows",underscored:"showed",underscoring:"showing"}],
 ["showcase","showcas(?:e|es|ed|ing)",{showcase:"show",showcases:"shows",showcased:"showed",showcasing:"showing"}],
 ["intricate","intricate",{intricate:"complex"}],
 ["multifaceted","multi-?faceted",{multifaceted:"complex","multi-faceted":"complex"}],
 ["myriad","myriad",{myriad:"many"}],
 ["plethora","plethora",null],
 ["vibrant","vibrant",null],
 ["elevate","elevat(?:e|es|ed|ing)",{elevate:"improve",elevates:"improves",elevated:"improved",elevating:"improving"}],
 ["unlock","unlock(?:s|ed|ing)?",null],
 ["harness","harness(?:es|ed|ing)?",{harness:"use",harnesses:"uses",harnessed:"used",harnessing:"using"}],
 ["holistic","holistic(?:ally)?",null],
 ["paradigm","paradigms?",{paradigm:"model",paradigms:"models"}],
 ["synergy","synerg(?:y|ies|istic)",null],
 ["game-changer","game[- ]?chang(?:er|ers|ing)",null],
 ["cutting-edge","cutting[- ]edge",{"cutting-edge":"new","cutting edge":"new"}],
 ["ever-evolving","ever[- ](?:evolving|changing|growing|expanding)",null],
 ["transformative","transformative",null],
 ["comprehensive","comprehensive",{comprehensive:"complete"}],
 ["meticulous","meticulous(?:ly)?",{meticulous:"careful",meticulously:"carefully"}],
 ["invaluable","invaluable",{invaluable:"useful"}],
 ["profound","profound(?:ly)?",null],
 ["resonate","resonat(?:e|es|ed|ing)",null],
 ["streamline","streamlin(?:e|es|ed|ing)",{streamline:"simplify",streamlines:"simplifies",streamlined:"simplified",streamlining:"simplifying"}],
 ["empower","empower(?:s|ed|ing|ment)?",null],
 ["facilitate","facilitat(?:e|es|ed|ing)",{facilitate:"help",facilitates:"helps",facilitated:"helped",facilitating:"helping"}],
 ["nuanced","nuanced",null],
 ["endeavor","endeavou?rs?",{endeavor:"effort",endeavors:"efforts",endeavour:"effort",endeavours:"efforts"}],
 ["interplay","interplay",null],
 ["ecosystem","ecosystems?",null],
 ["commendable","commendable",null],
 ["noteworthy","noteworthy",null],
 ["dynamic","dynamic",null],
 ["enhance","enhanc(?:e|es|ed|ing|ement)",{enhance:"improve",enhances:"improves",enhanced:"improved",enhancing:"improving",enhancement:"improvement"}],
 ["optimal","optimal",{optimal:"best"}],
 ["journey","journey",null],
 ["insights","actionable insights|key takeaways|deep[- ]dive",null],
];
words.forEach(([id,re,map])=>rule("word",id,id,B+"(?:"+re+")"+B,"Overused AI vocabulary. "+(map?"Swap for the plain word.":"Cut it or say the concrete thing."),map));

// ---- Stock phrases ----
const P=[
 ["worth-noting","It's worth noting","it(?:'s|’s| is) worth (?:noting|mentioning|pointing out|remembering|considering|emphasi[sz]ing|stressing|highlighting|flagging|adding|recalling|reiterating|repeating|keeping in mind|bearing in mind) that","Throat-clearing. State the point.",""],
 ["worth-any","Worth + …","worth\\s+(?!nothing|something|anything|everything)\\w+","Banned filler \"worth …\". State the point directly, or cut.",null],
 ["where-it-stands","Where it stands","where (?:it|this|that|we|things|matters) stands?","AI section-heading cliché. Name the actual takeaway.",null],
 ["deserves-any","Deserves + X","deserves?\\s+(?:emphasis|mention|mentioning|naming|attention|note|noting|scrutiny|flagging|highlighting|comment|repeating|a\\s+\\w+|to\\s+be\\s+\\w+|\\w+ing)","Throat-clearing \"deserves …\". Make the point directly.",null],
 ["important-to-note","It's important to note","it(?:'s|’s| is) (?:important|crucial|essential|vital) to (?:note|remember|recognize|understand|consider)(?: that)?","Throat-clearing. State the point.",""],
 ["todays-world","In today's world","in today(?:'s|’s) (?:fast-paced |digital |modern |ever-changing |rapidly evolving )?(?:world|age|landscape|era|environment|society)","Generic scene-setter.",""],
 ["end-of-day","At the end of the day","at the end of the day","Cliché.",""],
 ["when-it-comes","When it comes to","when it comes to","Wordy frame.","for"],
 ["plays-role","Plays a key role","play(?:s|ed|ing)? an? (?:crucial|pivotal|key|vital|significant|critical|important|central|major) role","Vague importance claim. Say what it does.",null],
 ["testament-to","A testament to","(?:a|is a|stands as a) testament to","Stock phrase.",null],
 ["conclusion","In conclusion","in (?:conclusion|summary)|to sum up|all in all","Essay scaffolding.",""],
 ["dive-in","Dive into","let(?:'s|’s) dive in|dive (?:deep |deeper )?into","Stock opener.","examine"],
 ["whether-youre","Whether you're X or Y","whether you(?:'re|’re| are) an? [^,.!?]{1,40}? or (?:an? )?[^,.!?]{1,40}?,","Audience-sweep opener.",null],
 ["serves-as","Serves as / stands as","(?:serv|stand)(?:es|ed|ing)? as","Inflated copula.",{"serves as":"is","stands as":"is","served as":"was","stood as":"was"}],
 ["boasts","Boasts","boast(?:s|ed|ing)?","Inflated verb.",{boasts:"has",boast:"have",boasted:"had",boasting:"having"}],
 ["look-no-further","Look no further","look no further","Ad copy cliché.",""],
 ["navigate-complex","Navigate the complexities","navigat(?:e|es|ed|ing) (?:the )?(?:complexities|challenges|intricacies|landscape|world|nuances)(?: of)?","Abstract journey metaphor.",null],
 ["wide-range","A wide range of","an? (?:wide|broad|diverse|vast) (?:range|array|variety|spectrum) of","Padding.","many"],
 ["world-of","The world of","the (?:world|realm) of","Padding.",""],
 ["unlock-power","Unlock the potential","unlock(?:ing)? (?:the |your |its )?(?:full )?(?:power|potential|secrets?)","Marketing cliché.",null],
 ["heres-the","Here's the thing","here(?:'s|’s) the (?:thing|kicker|deal|catch|truth)|here(?:'s|’s) (?:why|how)(?= *:)","Setup-for-reveal.",""],
 ["bottom-line","The bottom line","the bottom line","Cliché.",null],
 ["first-foremost","First and foremost","first and foremost","Doubled phrase.","first"],
 ["needless","Needless to say","needless to say","If it's needless, don't say it.",""],
 ["hope-helps","I hope this helps","i hope this (?:helps|email finds you well|finds you well)","Chatbot sign-off.",""],
 ["feel-free","Feel free to","feel free to","Chatbot filler.",""],
 ["great-q","Great question","great question[!.]?","Chatbot opener.",""],
 ["imagine-world","Imagine a world","imagine a (?:world|future)|in a world where","Cinematic opener.",null],
 ["rich-history","Rich history","(?:rich|storied|long-standing) (?:history|heritage|tradition)","Stock adjective pair.",null],
 ["ever-before","Like never before","like never before|than ever before","Hype filler.",""],
 ["game-changing","Take it to the next level","(?:take|taking|takes) (?:it|your \\w+) to the next level|pave(?:s|d)? the way","Hype cliché.",null],
 ["in-essence","In essence","in essence|at its core|simply put","Summary tic.",""],
 ["shed-light","Shed light on","sh(?:ed|eds|edding) (?:some )?light on","Stock metaphor.","explain"],
];
P.forEach(([id,label,re,msg,fix])=>rule("phrase",id,label,B+"(?:"+re+")"+(re.endsWith(",")||re.endsWith("?")?"":B),msg,fix));

// ---- Constructions ----
rule("construct","not-just","Not just X, but Y",B+"(?:not just|not only|not merely|not simply)\\b[^.!?;]{1,80}?(?:,|;|—|–| -)\\s*(?:but|it(?:'s|’s| is)|they(?:'re| are))\\b(?: also)?","Contrast-reframe formula. Make the claim directly.",null);
rule("construct","isnt-just","Isn't just…","\\b(?:is|are|was|were)(?:n't|n’t| not) (?:just|only|merely|simply)\\b","Reframe setup. Say what it is.",null,2);
rule("construct","its-not-its","It's not X, it's Y",B+"it(?:'s|’s| is) not (?:about )?[^.!?]{1,60}?(?:,|;|—|–|\\.)\\s*it(?:'s|’s| is)\\b","Contrast-reframe formula.",null);
rule("construct","q-reveal","Rhetorical question → reveal",B+"(?:the (?:result|answer|catch|problem|secret|truth|kicker|best part|twist|key|reason|outcome)|why does (?:this|it|that) matter|so what(?:'s| does)? [^?]{0,30})\\?","Fake suspense. Just state it.",null);
rule("construct","colon-reveal","Colon reveal",B+"(?:the (?:key|secret|answer|truth|reality|lesson|takeaway|verdict|result|problem)|one thing|the short answer)(?: is)?:","Setup-colon-payoff. State it plainly.",null);
rule("construct","ing-tail","Tacked-on -ing clause",",\\s+(?:ensuring|highlighting|underscoring|showcasing|making it|allowing|enabling|reflecting|emphasizing|demonstrating|fostering|paving the way|creating|providing|offering|solidifying|cementing|marking|signaling|illustrating|contributing)\\b","Trailing participle that adds vague commentary. End the sentence or give it its own.",null);
rule("construct","vague-this","Vague \"This\" opener","(?<=^|[.!?]\\s+|\\n)This (?:ensures|allows|enables|means|helps|makes|creates|provides|highlights|underscores|demonstrates|shows|is (?:why|what|where|how))\\b","\"This\" with no noun. Name what \"this\" is.",null,2);
rule("construct","from-to","From X to Y sweep",B+"from [a-z][\\w' -]{1,30} to [a-z][\\w' -]{1,30},","Sweeping range construction.",null,1.5);
rule("construct","triplet","Adjective triplet",B+"[a-z]+(?:ive|al|ful|ous|ent|ant|ic|ble|less|ing|ed),\\s+[a-z]+(?:ive|al|ful|ous|ent|ant|ic|ble|less|ing|ed),?\\s+and\\s+[a-z]+(?:ive|al|ful|ous|ent|ant|ic|ble|less|ing|ed)"+B,"Rule-of-three list. Keep the one that matters.",null,2);
rule("construct","em-dash","Em-dash aside","\\s?[—–]\\s?|\\s--\\s","Em-dash asides are a strong AI tell. Use a comma, period, or parentheses.",", ",1);
rule("construct","sentence-transition","Transition opener","(?<=^|[.!?]\\s+|\\n)(?:Additionally|Furthermore|Moreover|Ultimately|Importantly|Notably|Overall|Consequently|Crucially|Interestingly|Indeed|Thus|Hence|Essentially|Undoubtedly|Ultimately),\\s*","Mechanical transition at sentence start.","",1.5,{cap:true});

// ---- Filler ----
rule("filler","intensifier","Intensifier",B+"(?:truly|really|incredibly|deeply|extremely|highly|very|quite|remarkably|exceptionally|absolutely|undoubtedly|genuinely|literally|super)\\s+(?=[a-z])","Intensifier props up a weak word. Cut it or choose a stronger word.","");
rule("filler","hedge","Hedge",B+"(?:arguably|essentially|basically|potentially|somewhat|perhaps|generally speaking|to some extent|in many ways|it could be argued that|it seems that)\\b,?\\s*","Hedging softens the claim. Commit or qualify with a fact.","");
rule("filler","just","Just / actually / simply",B+"(?:just|actually|simply|merely)\\s+(?=[a-z])","Usually filler.","",0.5);

// ---- Grammar & wordiness ----
const G=[
 ["in-order-to","In order to","in order to","to"],
 ["due-to-fact","Due to the fact that","due to the fact that|owing to the fact that","because"],
 ["prior-to","Prior to","prior to","before"],
 ["number-of","A number of","a number of","several"],
 ["fact-that","The fact that","the fact that",null],
 ["in-terms-of","In terms of","in terms of",null],
 ["this-point","At this point in time","at this point in time|at the present time|at this moment in time","now"],
 ["able-to","Is able to","(?:is|are) able to",{"is able to":"can","are able to":"can"}],
 ["ability-to","Has the ability to","ha(?:s|ve) the (?:ability|capacity) to","can"],
 ["purpose-of","For the purpose of","for the purpose of","to"],
 ["event-that","In the event that","in the event that","if"],
 ["with-regard","With regard to","with (?:regard|respect) to|in regard to|in relation to","about"],
 ["each-every","Each and every","each and every","every"],
 ["clear-that","It is clear that","it is (?:clear|evident|obvious|apparent) that",""],
 ["very-unique","Very unique","(?:very|most|quite) unique","unique"],
 ["alot","alot","alot","a lot"],
 ["could-of","Could of","(could|should|would|must) of",'$1 have'],
 ["irregardless","Irregardless","irregardless","regardless"],
 ["moving-forward","Moving forward","moving forward|going forward",""],
 ["despite-fact","Despite the fact that","despite the fact that|in spite of the fact that","although"],
 ["whether-or-not","Whether or not","whether or not","whether"],
];
G.forEach(([id,label,re,fix])=>rule("grammar",id,label,B+"(?:"+re+")"+B,"Wordy. Shorter form says the same thing.",fix));
rule("grammar","passive","Passive voice",B+"(?:is|are|was|were|be|been|being)\\s+(?:\\w+ly\\s+)?(?:[a-z]+ed|built|made|given|taken|shown|seen|done|known|written|found|held|led|sent|set|told)\\b(?:\\s+by\\b)?","Passive voice. Name who does it.",null,0.5);
rule("grammar","repeat","Repeated word",B+"(\\w+)\\s+\\1"+B,"Word repeated.","$1",2);
rule("grammar","double-space","Double space","(?<=\\S) {2,}(?=\\S)","Extra spaces."," ",0.5);
rule("grammar","space-punct","Space before punctuation","(?<=\\w)\\s+(?=[,.;:!?](?:\\s|$))","Stray space before punctuation.","",0.5);

// compile
R.forEach(r=>{r.re=new RegExp(r.src,"gi")});

// ---- Analysis ----
function sentences(text){
  const out=[];const re=/[^.!?\n]+(?:[.!?]+["'”’)\]]*|\n|$)/g;let m;
  while((m=re.exec(text))){const s=m[0];const lead=s.match(/^\s*/)[0].length;const body=s.trim();if(!body){if(m[0]==="")re.lastIndex++;continue}
    const wc=(body.match(/[A-Za-z0-9’']+/g)||[]).length;if(wc)out.push({start:m.index+lead,end:m.index+lead+body.length,words:wc});}
  return out;
}
let ignored=new Set();
function analyze(text){
  const hits=[];
  R.forEach(r=>{r.re.lastIndex=0;let m;while((m=r.re.exec(text))){if(!m[0].length){r.re.lastIndex++;continue}
    const t=m[0];
    // Proper-noun guard: a token with 2+ uppercase letters is almost always an acronym
    // or trial name (DYNAMIC, EMERALD, PROfound), not AI slop. Skip for "word" rules.
    if(r.cat==="word"&&(t.match(/[A-Z]/g)||[]).length>=2)continue;
    const key=r.id+"|"+t.toLowerCase().trim();if(ignored.has(key))continue;
    hits.push({rule:r,start:m.index,end:m.index+t.length,text:t,groups:m.slice(1)});}});
  const sents=sentences(text);
  sents.forEach(s=>{if(s.words>35){const r=LONG;hits.push({rule:r,start:s.start,end:s.end,text:text.slice(s.start,s.end),groups:[]})}});
  // resolve overlaps: prefer higher weight, then longer
  hits.sort((a,b)=>a.start-b.start||(b.rule.w-a.rule.w)||(b.end-b.start)-(a.end-a.start));
  const kept=[];const longs=[];
  hits.forEach(h=>{if(h.rule.id==="long"){longs.push(h);return}
    const last=kept[kept.length-1];
    if(last&&h.start<last.end){if(h.rule.w>last.rule.w){kept[kept.length-1]=h}return}
    kept.push(h)});
  const words=(text.match(/[A-Za-z0-9’']+/g)||[]).length;
  const lens=sents.map(s=>s.words);const mean=lens.length?lens.reduce((a,b)=>a+b,0)/lens.length:0;
  const sd=lens.length>1?Math.sqrt(lens.reduce((a,b)=>a+(b-mean)**2,0)/(lens.length-1)):0;
  const cv=mean?sd/mean:0;
  const dashes=(text.match(/—|–|\s--\s/g)||[]).length;
  let weight=kept.reduce((a,h)=>a+h.rule.w,0)+longs.length*LONG.w;
  const uniform=lens.length>=6&&cv<0.35;
  if(uniform)weight+=4;
  const density=words?weight/words*100:0;
  const score=words?Math.round(100*(1-Math.exp(-density/8))):0;
  return {hits:kept,longs,words,sents:lens.length,mean,cv,dashes,uniform,score};
}
const LONG={cat:"rhythm",id:"long",label:"Long sentence",msg:"Over 35 words. Split it.",fix:null,w:1};
const UNI={cat:"rhythm",id:"uniform",label:"Uniform rhythm",msg:"Sentence lengths barely vary, a common machine-text signature. Mix short and long sentences.",fix:null,w:4};

function matchCase(orig,rep){if(!rep)return rep;
  if(orig===orig.toUpperCase()&&/[A-Z]/.test(orig)&&orig.length>1)return rep.toUpperCase();
  if(/^[A-Z]/.test(orig))return rep[0].toUpperCase()+rep.slice(1);return rep;}
function fixFor(h){const f=h.rule.fix;if(f===null||f===undefined)return null;
  if(typeof f==="object"){const k=h.text.toLowerCase();const v=f[k];return v===undefined?null:matchCase(h.text,v)}
  let rep=f.replace(/\$(\d)/g,(_,i)=>h.groups[i-1]||"");
  if(h.rule.id==="em-dash"||h.rule.id==="double-space"||h.rule.id==="space-punct"||h.rule.id==="repeat")return rep;
  return matchCase(h.text.trim(),rep);}
function applyOne(text,h,rep){
  let s=h.start,e=h.end;let before=text.slice(0,s),after=text.slice(e);
  if(rep===""){ // cut: tidy surrounding space and capitalization
    if(/[ \t]$/.test(before)&&/^[ \t]/.test(after))after=after.replace(/^[ \t]+/,"");
    if(/[ \t]$/.test(before)&&/^[,.;:!?]/.test(after))before=before.replace(/[ \t]+$/,"");
    after=after.replace(/^,\s*/,before.trim()===""||/[.!?\n]\s*$/.test(before)?"":", ");
    if(before.trim()===""||/[.!?\n]\s*$/.test(before))after=after.replace(/^(\s*)([a-z])/,(m,a,c)=>a+c.toUpperCase());
    if(before.trim()===""||/[.!?\n]\s*$/.test(before)) after=after.replace(/^[ \t]+/,"");
  } else if(h.rule.id==="em-dash"){before=before.replace(/[ \t]+$/,"");after=after.replace(/^[ \t]+/,"")}
  return before+rep+after;
}


// ---------------- CLI ----------------
const MASK = "\u0001";
// Hide everything that isn't prose, keeping offsets identical so fixes map back to the original.
function maskMarkdown(src) {
  const blank = s => s.replace(/[^\n]/g, MASK);
  let t = src;
  t = t.replace(/^---\r?\n[\s\S]*?\n---\r?\n/, blank);                    // YAML frontmatter (CRLF-tolerant)
  t = t.replace(/^(```|~~~)[^\n]*\n[\s\S]*?\n\1[^\n]*$/gm, blank);         // fenced code
  t = t.replace(/`[^`\n]+`/g, blank);                                      // inline code
  t = t.replace(/<!--[\s\S]*?-->/g, blank);                                // comments
  t = t.replace(/<\/?[a-zA-Z][^>\n]*>/g, blank);                           // html/mdx tags
  t = t.replace(/(\]\()([^)\n]*)(\))/g, (m, a, b, c) => a + blank(b) + c); // link targets
  t = t.replace(/^\[[^\]\n]+\]:\s*\S+.*$/gm, blank);                       // reference links
  t = t.replace(/https?:\/\/\S+/g, blank);                                 // bare urls
  t = t.replace(/^\s{0,3}(#{1,6}|>|[-*+]|\d+[.)])\s/gm, blank);            // block markers
  t = t.replace(/^\|?\s*:?-{3,}.*$/gm, blank);                             // table rules
  return t;
}

function lineCol(src, i) {
  const before = src.slice(0, i);
  const line = before.split("\n").length;
  return [line, i - before.lastIndexOf("\n")];
}

function walk(p, out) {
  const st = fs.statSync(p);
  if (st.isDirectory()) {
    for (const f of fs.readdirSync(p)) {
      if (f === "node_modules" || f.startsWith(".")) continue;
      walk(path.join(p, f), out);
    }
  } else if (/\.mdx?$/i.test(p)) out.push(p);
  return out;
}

function checkFile(file, { fix }) {
  const src = fs.readFileSync(file, "utf8");
  const res = analyze(maskMarkdown(src));
  const findings = [];
  const add = (h, rule) => {
    const [line, col] = lineCol(src, h.start);
    const f = rule.fix === null ? null : fixFor(h);
    findings.push({ rule: rule.id, category: rule.cat, label: rule.label, line, col,
      text: src.slice(h.start, h.end).replace(/\s+/g, " ").trim().slice(0, 120),
      fix: f, weight: rule.w });
  };
  res.hits.forEach(h => add(h, h.rule));
  res.longs.forEach(h => add(h, LONG));
  findings.sort((a, b) => a.line - b.line || a.col - b.col);
  let fixed = 0;
  if (fix) {
    let t = src;
    res.hits.filter(h => fixFor(h) !== null).sort((a, b) => b.start - a.start)
      .forEach(h => { t = applyOne(t, h, fixFor(h)); fixed++; });
    if (fixed) fs.writeFileSync(file, t);
  }
  return { file, score: res.score, words: res.words, sentences: res.sents,
    rhythmVariation: +res.cv.toFixed(2), uniformRhythm: res.uniform,
    emDashesPer100: res.words ? +(res.dashes / res.words * 100).toFixed(2) : 0,
    findings, fixed };
}

const verdict = s => s < 15 ? "clean" : s < 35 ? "light" : s < 60 ? "noticeable" : "heavy";

function main() {
  const args = process.argv.slice(2);
  const opt = { fix: false, json: false, min: 60, top: 15, summary: false };
  const paths = [];
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--fix") opt.fix = true;
    else if (a === "--json") opt.json = true;
    else if (a === "--summary") opt.summary = true;
    else if (a === "--min") opt.min = +args[++i];
    else if (a === "--top") opt.top = +args[++i];
    else if (a === "-h" || a === "--help") { console.log(fs.readFileSync(new URL(import.meta.url), "utf8").split("\n").slice(1, 11).join("\n").replace(/^\/\/ ?/gm, "")); return; }
    else paths.push(a);
  }
  if (!paths.length) { console.error("usage: node slop.mjs <file-or-folder> [--fix] [--json] [--min N] [--top N] [--summary]"); process.exit(2); }
  const files = paths.flatMap(p => walk(p, []));
  const results = files.map(f => checkFile(f, opt));
  const failing = results.filter(r => r.score >= opt.min);

  if (opt.json) {
    console.log(JSON.stringify({ threshold: opt.min, files: results }, null, 2));
  } else {
    for (const r of [...results].sort((a, b) => b.score - a.score)) {
      console.log(`${String(r.score).padStart(3)}  ${verdict(r.score).padEnd(10)} ${String(r.words).padStart(5)}w  ${r.findings.length} flags${r.fixed ? `, ${r.fixed} fixed` : ""}${r.uniformRhythm ? ", uniform rhythm" : ""}  ${r.file}`);
      if (opt.summary) continue;
      const list = opt.top ? r.findings.slice(0, opt.top) : r.findings;
      for (const f of list) {
        const sug = f.fix === null ? "" : f.fix === "" ? "  → cut" : `  → "${f.fix.trim() || f.fix}"`;
        console.log(`       ${String(f.line).padStart(4)}:${String(f.col).padEnd(3)} ${f.category.padEnd(9)} ${f.label}: "${f.text}"${sug}`);
      }
      if (opt.top && r.findings.length > opt.top) console.log(`       … ${r.findings.length - opt.top} more (use --top 0)`);
      console.log("");
    }
    console.log(`${results.length} file(s), ${failing.length} at or above ${opt.min}.`);
  }
  process.exitCode = failing.length ? 1 : 0;
}
main();
