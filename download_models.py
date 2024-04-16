# -*- coding: utf-8 -*-

import wget
import os

# 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

def download_biobert():
    folder = "biobert-v1.1"
    if not os.path.exists(folder):
        os.makedirs(folder)
    config = "https://huggingface.co/dmis-lab/biobert-v1.1/resolve/main/config.json"
    flax_model = "https://huggingface.co/dmis-lab/biobert-v1.1/resolve/main/flax_model.msgpack"
    pytorch_model = "https://huggingface.co/dmis-lab/biobert-v1.1/resolve/main/pytorch_model.bin"
    special_tokens = "https://huggingface.co/dmis-lab/biobert-v1.1/resolve/main/special_tokens_map.json"
    tokenizer_json = "https://huggingface.co/dmis-lab/biobert-v1.1/resolve/main/tokenizer_config.json"
    vocab = "https://huggingface.co/dmis-lab/biobert-v1.1/resolve/main/vocab.txt"
    
    wget.download(config, os.path.join(folder,"config.json"))
    wget.download(flax_model, os.path.join(folder,"flax_model.msgpack"))
    wget.download(pytorch_model, os.path.join(folder,"pytorch_model.bin"))
    wget.download(special_tokens, os.path.join(folder,"special_tokens_map.json"))
    wget.download(tokenizer_json, os.path.join(folder,"tokenizer_config.json"))
    wget.download(vocab, os.path.join(folder,"vocab.txt"))
    return


def download_paraphrase_multilingual():
    folder = "paraphrase-multilingual-MiniLM-L12-v2"
    if not os.path.exists(folder):
        os.makedirs(folder)
    config = "https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/resolve/main/config.json"
    config_sentence_transformers = "https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/resolve/main/config_sentence_transformers.json"
    modules = "https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/resolve/main/modules.json"
    pytorch_model = "https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/resolve/main/pytorch_model.bin"
    sentence_bert_config = "https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/resolve/main/sentence_bert_config.json"
    sentencepiece = "https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/resolve/main/sentencepiece.bpe.model"
    special_tokens_map = "https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/resolve/main/special_tokens_map.json"
    tf_modelh5 = "https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/resolve/main/tf_model.h5"
    tokenizer = "https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/resolve/main/tokenizer.json"
    tokenizer_config = "https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/resolve/main/tokenizer_config.json"
    unigram = "https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/resolve/main/unigram.json"

    wget.download(config, os.path.join(folder, "config.json"))
    wget.download(config_sentence_transformers, os.path.join(folder,"config_sentence_transformers.json"))
    wget.download(modules, os.path.join(folder,"modules.json"))
    wget.download(pytorch_model, os.path.join(folder,"pytorch_model.bin"))
    wget.download(sentence_bert_config, os.path.join(folder,"sentence_bert_config.json"))
    wget.download(sentencepiece, os.path.join(folder,"sentencepiece.bpe.model"))
    wget.download(special_tokens_map, os.path.join(folder,"special_tokens_map.json"))
    wget.download(tf_modelh5, os.path.join(folder,"tf_model.h5"))
    wget.download(tokenizer, os.path.join(folder,"tokenizer.json"))
    wget.download(tokenizer_config, os.path.join(folder,"tokenizer_config.json"))
    wget.download(unigram, os.path.join(folder,"unigram.json"))

    if not os.path.exists(os.path.join(folder, "1_Pooling")):
        os.makedirs(os.path.join(folder, "1_Pooling"))
    pooling_config = "https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/resolve/main/1_Pooling/config.json"
    wget.download(pooling_config, os.path.join(folder, "1_Pooling", "config.json"))
    return


def download_all_mini():
    folder = "all-MiniLM-L6-v2"
    if not os.path.exists(folder):
        os.makedirs(folder)
    config = "https://huggingface.co/Xenova/all-MiniLM-L6-v2/resolve/main/config.json"
    special_tokens_map = "https://huggingface.co/Xenova/all-MiniLM-L6-v2/resolve/main/special_tokens_map.json"
    tokenizer = "https://huggingface.co/Xenova/all-MiniLM-L6-v2/resolve/main/tokenizer.json"
    tokenizer_config = "https://huggingface.co/Xenova/all-MiniLM-L6-v2/resolve/main/tokenizer_config.json"
    vocab = "https://huggingface.co/Xenova/all-MiniLM-L6-v2/resolve/main/vocab.txt"

    wget.download(config, os.path.join(folder, "config.json"))
    wget.download(special_tokens_map, os.path.join(folder,"special_tokens_map.json"))
    wget.download(tokenizer, os.path.join(folder,"tokenizer.json"))
    wget.download(tokenizer_config, os.path.join(folder,"tokenizer_config.json"))
    wget.download(vocab, os.path.join(folder,"vocab.json"))

    if not os.path.exists(os.path.join(folder, "1_Pooling")):
        os.makedirs(os.path.join(folder, "1_Pooling"))
    pooling_config = "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/1_Pooling/config.json"
    wget.download(pooling_config, os.path.join(folder, "1_Pooling", "config.json"))
    return


if __name__ == "__main__":
    download_biobert()
    download_paraphrase_multilingual()
    download_all_mini()