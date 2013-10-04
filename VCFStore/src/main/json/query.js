// This is a quick example of a query against our VCF repository.

// First, ensure the indices.  This will take some time if not indexed
db.publicvariants.ensureIndex( { "POS": 1 }, { background: true } )
// Remember to background your index call

// Here is our range definition
var begin = 10000;
var end = 10200;

// The Chromosome position is fuzzy in format so, we use a regex to locate all
var chromosome = ".*17$";
var variant = "A"

// Query for range and Chromosome Position.
db.publicvariants.find(
    {"POS":{$gte: begin, $lt: end},
        "CHROM":{$regex : chromosome}
    })

db.variants.find(
    {"POS":{$gte: begin, $lt: end},
        "CHROM":{$regex : chromosome}
    })

// Query for a specific variant in a range
db.publicvariants.find(
    {"POS":{$gte: begin, $lt: end},
        "CHROM":{$regex : chromosome},
        "ALT":variant})

db.variants.find(
    {"POS":{$gte: begin, $lt: end},
        "CHROM":{$regex : chromosome},
        "ALT":variant})