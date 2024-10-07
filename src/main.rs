use dotenv::dotenv;
use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use mongodb::{Client, IndexModel};
use regex::Regex;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

fn is_sentence_boundary(text: &str, pos: usize) -> bool {
    if pos == 0 || pos >= text.len() - 1 {
        return false;
    }

    let prev_char = text.chars().nth(pos - 1).unwrap();
    let next_char = text.chars().nth(pos + 1).unwrap();

    // Check if it's not part of an abbreviation, number, or ellipsis
    if prev_char.is_alphabetic() && next_char.is_alphabetic() {
        return false;
    }

    if prev_char.is_numeric() && next_char.is_numeric() {
        return false;
    }

    if next_char == '.' {
        return false; // Likely an ellipsis
    }

    // Check if it's followed by a space and a capital letter or number
    next_char.is_whitespace()
        && text[pos + 1..]
            .trim_start()
            .chars()
            .next()
            .map_or(false, |c| c.is_uppercase() || c.is_numeric())
}

fn is_valid_sentence(sentence: &str) -> bool {
    let trimmed = sentence.trim();
    if trimmed.is_empty() {
        return false;
    }

    // A valid sentence should have at least 3 words and not be just a number
    let word_count = trimmed.split_whitespace().count();
    word_count >= 3 && !trimmed.parse::<f64>().is_ok() && trimmed.len() >= 10
}

fn split_into_sentences(text: &str) -> Vec<String> {
    let mut sentences = Vec::new();
    let mut current_sentence = String::new();
    let mut chars = text.char_indices().peekable();

    while let Some((i, c)) = chars.next() {
        current_sentence.push(c);

        if c == '.' && is_sentence_boundary(text, i) {
            if is_valid_sentence(&current_sentence) {
                sentences.push(current_sentence.trim().to_string());
                current_sentence.clear();
            }
        } else if c == '?' || c == '!' {
            // Check if it's not part of a quotation
            if chars.peek().map_or(true, |&(_, next_c)| {
                next_c.is_whitespace() || next_c.is_uppercase()
            }) {
                if is_valid_sentence(&current_sentence) {
                    sentences.push(current_sentence.trim().to_string());
                    current_sentence.clear();
                }
            }
        }
    }

    // Handle the last sentence
    if !current_sentence.is_empty() && is_valid_sentence(&current_sentence) {
        sentences.push(current_sentence.trim().to_string());
    } else if let Some(last) = sentences.last_mut() {
        // If the last part isn't a valid sentence, append it to the previous one
        last.push(' ');
        last.push_str(current_sentence.trim());
    }

    sentences
}

struct SentenceWriter {
    writer: BufWriter<File>,
}

impl SentenceWriter {
    fn new(filename: &str) -> std::io::Result<Self> {
        let file = File::create(filename)?;
        let writer = BufWriter::new(file);
        Ok(SentenceWriter { writer })
    }

    fn write_sentence(&mut self, sentence: &str) -> std::io::Result<()> {
        writeln!(self.writer, "{}", sentence)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let file_path = std::env::var("INPUT_FILE_PATH").expect("INPUT_FILE_PATH must be set");
    let mongodb_uri = std::env::var("MONGODB_URI").expect("MONGODB_URI must be set");
    let output_file_path = std::env::var("OUTPUT_FILE_PATH").expect("OUTPUT_FILE_PATH must be set");

    let client_options = ClientOptions::parse(&mongodb_uri).await?;
    let client = Client::with_options(client_options)?;

    let db = client.database("coca_like_db");
    let collection: mongodb::Collection<mongodb::bson::Document> = db.collection("corpus");

    // Create a text index for efficient searching
    let index_model = IndexModel::builder().keys(doc! { "text": "text" }).build();
    collection.create_index(index_model, None).await?;

    let file = File::open(&file_path)?;
    let reader = BufReader::new(file);
    let mut sentence_writer = SentenceWriter::new(&output_file_path)?;

    // Compile regex once
    let re = Regex::new(r"[^a-zA-Z0-9\s.!?]").unwrap();

    let mut batch = vec![];
    let batch_size = 1000; // Adjust as needed
    let mut sentence_count = 0;

    for (line_number, line_result) in reader.lines().enumerate() {
        let line =
            line_result.map_err(|e| format!("Error reading line {}: {}", line_number + 1, e))?;
        println!("Processing line {}: {}", line_number + 1, line);

        // Remove unwanted characters
        let cleaned_line = re.replace_all(&line, "").trim().to_string();

        // Split the cleaned line into sentences
        let sentences = split_into_sentences(&cleaned_line);

        for sentence in sentences {
            if !sentence.is_empty() {
                batch.push(doc! {
                    "text": sentence.clone(),
                    "fileName": file_path.clone(),
                    "lineNumber": (line_number + 1) as i32
                });

                println!("Processed sentence: {}", sentence);

                // sentence_writer.write_sentence(&sentence)?;

                if batch.len() >= batch_size {
                    collection.insert_many(batch.clone(), None).await?;
                    batch.clear();
                    println!("Uploaded {} sentences", sentence_count + batch_size);

                    // Flush the writer periodically
                    sentence + _writer.flush()?;
                }

                sentence_count += 1;
            }
        }
    }

    // Handle any remaining items in the batch
    if !batch.is_empty() {
        collection.insert_many(batch.clone(), None).await?;
        println!("Uploaded final {} sentences", batch.len());
    }

    // Final flush to ensure all data is written
    sentence_writer.flush()?;

    println!("Total processed sentences: {}", sentence_count);

    Ok(())
}
