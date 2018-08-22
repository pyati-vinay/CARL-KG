Completeness Aware Rule Learning
================================
Mining in KGs have high degree of incompleteness, which may provide inaccurate quality of mined rules, The effort of this algorithm is to expose that incompleteness by introducing aware scoring functions. First the algorithm count number of triples per relations and number of entities, then count the number of missing triples per relation and compute support and other metric for each possible rule and body.
Note: This algorithm is not dependent on any 3rd party library such AMIE.


