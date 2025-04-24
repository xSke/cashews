from cashews import utils, DATA_DIR

nouns = utils.get_object("nouns", "nouns")
adjectives = utils.get_object("adjectives", "adjectives")
with open(f"{DATA_DIR}/nouns.txt", "w") as f:
    f.write("\n".join(nouns))
with open(f"{DATA_DIR}/adjectives.txt", "w") as f:
    f.write("\n".join(adjectives))
