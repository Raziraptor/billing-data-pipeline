using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace BillingDataPipeline
{
    public class PalantirUploader
    {
        // En la vida real, estas variables se leen de forma segura desde la nube (ej. Azure Key Vault)
        private static readonly string PalantirApiUrl = "https://api.palantirfoundry.com/v1/datasets/ingest";
        private static readonly string ApiToken = Environment.GetEnvironmentVariable("PALANTIR_API_TOKEN") ?? "simulated-token-12345";

        public static async Task UploadParquetPayloadAsync(string filePath)
        {
            if (!File.Exists(filePath))
            {
                Console.WriteLine($"Error: File not found at {filePath}");
                return;
            }

            Console.WriteLine("Initiating secure upload to Palantir Foundry...");

            using (HttpClient client = new HttpClient())
            {
                // Configuración de autenticación y cabeceras
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", ApiToken);
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                using (var fileStream = File.OpenRead(filePath))
                using (var content = new StreamContent(fileStream))
                {
                    // Especificamos que estamos enviando un archivo binario genérico
                    content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

                    try
                    {
                        // POST asíncrono a la API de Palantir
                        HttpResponseMessage response = await client.PostAsync(PalantirApiUrl, content);

                        if (response.IsSuccessStatusCode)
                        {
                            Console.WriteLine("Success: Data payload successfully ingested into Palantir Foundry.");
                        }
                        else
                        {
                            Console.WriteLine($"Upload Failed. Status Code: {response.StatusCode}");
                            string errorMsg = await response.Content.ReadAsStringAsync();
                            Console.WriteLine($"Details: {errorMsg}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Critical Error during upload: {ex.Message}");
                    }
                }
            }
        }

        public static async Task Main(string[] args)
        {
            string targetFile = "palantir_ingestion_payload.parquet";
            await UploadParquetPayloadAsync(targetFile);
        }
    }
}
