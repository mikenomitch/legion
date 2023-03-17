use hello::greeter_client::GreeterClient;
use hello::HelloRequest;

pub mod hello {
    tonic::include_proto!("hello");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = GreeterClient::connect("http://[::1]:50052").await?;

    let request = tonic::Request::new(HelloRequest {
        name: "Mike".into(),
    });

    let response = client.say_hello(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
