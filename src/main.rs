use reqwest::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let url = "https://nbg1-speed.hetzner.com/100MB.bin";

    let client = Client::new();

    println!("Downloading first 1MB...");

    let response = client
        .get(url)
        .header("Range", "bytes=0-999999")
        .send()
        .await?;

    let bytes = response.bytes().await?;

    std::fs::write("part.bin", &bytes)?;

    println!("Downloaded {} bytes", bytes.len());

    Ok(())
}