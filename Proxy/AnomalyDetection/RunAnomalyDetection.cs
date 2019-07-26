using System;
using System.IO;
using System.Text;
using System.Net.Http.Headers;
using Microsoft.AspNetCore.Mvc.Formatters;
using System.Net.Http;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Azure.Services.AppAuthentication;

namespace IrisFoundations.Function
{
    public static class RunAnomalyDetection
    {

        private static readonly HttpClient client = new HttpClient();

        [FunctionName("RunAnomalyDetection")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {

            var secretVal = await GetSecret();
            var secret = new Dictionary<string, string>
            {
                {"secret", secretVal}
            };

            var content = new StringContent(JsonConvert.SerializeObject(secret), Encoding.UTF8, "application/json");;
            var response = new HttpResponseMessage();

            response = await client.PostAsync("https://irisanomalydetection.azurewebsites.net/api/RunAnomalyDetection?threshold=3", content);
            return (ActionResult) new OkObjectResult(response.Content.ReadAsStringAsync());
        }

        public static async Task<string> GetSecret()
        {
            try{
                AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
                KeyVaultClient keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
                var secret = await keyVaultClient.GetSecretAsync("https://irisastar.vault.azure.net/", "AStarADFSecret").ConfigureAwait(false);
                return secret.Value;
            }

            catch(KeyVaultErrorException keyVaultErrorException)
            {
                return keyVaultErrorException.ToString();
            }
        }
    }
}
