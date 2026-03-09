use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::{self, Read};

#[derive(Deserialize)]
struct SentimentRequest {
    text: String,
}

#[derive(Serialize, Deserialize)]
struct SentimentResponse {
    text: String,
    normalized_text: String,
    score: i32,
    sentiment: String,
    positive_matches: Vec<String>,
    negative_matches: Vec<String>,
}

fn normalize_text(text: &str) -> String {
    text.to_lowercase()
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c.is_whitespace() {
                c
            } else {
                ' '
            }
        })
        .collect::<String>()
}

fn analyze_sentiment(text: &str) -> SentimentResponse {
    let positive_words: HashSet<&str> = [
        "good", "great", "excellent", "amazing", "love", "happy",
        "awesome", "fast", "smooth", "best", "nice", "perfect",
    ]
    .iter()
    .copied()
    .collect();

    let negative_words: HashSet<&str> = [
        "bad", "terrible", "awful", "hate", "sad", "poor",
        "worst", "slow", "broken", "laggy", "issue", "problem",
    ]
    .iter()
    .copied()
    .collect();

    let normalized = normalize_text(text);

    let mut score = 0;
    let mut positive_matches = Vec::new();
    let mut negative_matches = Vec::new();

    for word in normalized.split_whitespace() {
        if positive_words.contains(word) {
            score += 1;
            positive_matches.push(word.to_string());
        }

        if negative_words.contains(word) {
            score -= 1;
            negative_matches.push(word.to_string());
        }
    }

    let sentiment = if score > 0 {
        "Positive"
    } else if score < 0 {
        "Negative"
    } else {
        "Neutral"
    };

    SentimentResponse {
        text: text.to_string(),
        normalized_text: normalized,
        score,
        sentiment: sentiment.to_string(),
        positive_matches,
        negative_matches,
    }
}

fn main() {
    let mut input = String::new();

    if let Err(err) = io::stdin().read_to_string(&mut input) {
        eprintln!("Failed to read input: {}", err);
        std::process::exit(1);
    }

    let request: SentimentRequest = match serde_json::from_str(&input) {
        Ok(req) => req,
        Err(err) => {
            eprintln!("Invalid JSON input: {}", err);
            std::process::exit(1);
        }
    };

    let response = analyze_sentiment(&request.text);

    match serde_json::to_string(&response) {
        Ok(json) => println!("{}", json),
        Err(err) => {
            eprintln!("Failed to serialize response: {}", err);
            std::process::exit(1);
        }
    }
}