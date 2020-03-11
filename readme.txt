text-fingerprint
----------------
Teach program to build a database of word pairs.
Analyze a text from the same context to see its fingerprint and in what way it is different from the taught sample.

possible contexts: english novels, spanish facebook comments, chinese newspaper articles, reddit (mostly english), etc.

Measurement/combination (data) is two words and how far the second follows the first, measured in words.

example: "This is a good day to die." -> (this,is,1), (is,a,1), (a,good,1), (good,day,1), (day,to,1), (to,die,1), (die,'.',1)
wider context could be: (this,a,2), (is,good,2), etc.

So a person/bot could be fingerprinted like how often they use a certain combination. For example:
"how often do they use" has an additional "do" word, which means (often,they,1) will not appear, but (often,they,2) will.