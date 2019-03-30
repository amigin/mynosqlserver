using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Api.Hubs;
using MyNoSqlServer.Api.Services;
using MyNoSqlServer.AzureStorage;
using MyNoSqlServer.Domains;
using Swashbuckle.AspNetCore.Swagger;

namespace MyNoSqlServer.Api
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            
            services.AddApplicationInsightsTelemetry(Configuration);
            
            services.AddMvc().AddJsonOptions(options =>
            {
                options.SerializerSettings.ContractResolver
                    = new Newtonsoft.Json.Serialization.DefaultContractResolver();
            });
            
            services.AddSignalR(); 

            // Register the Swagger generator, defining 1 or more Swagger documents
            services.AddSwaggerGen(c => { c.SwaggerDoc("v1", new Info {Title = "MyNoSql API", Version = "v1"}); });

            var settings = SettingsLoader.LoadSettings();
            
            settings.BackupAzureConnectString.BindAzureStorage();

            ServiceLocator.Synchronizer.DbRowSynchronizer = new DbRowSynchronizerToSignalR();

        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                app.UseHsts();
            }
            
            // Enable middleware to serve generated Swagger as a JSON endpoint.
            app.UseSwagger();

            // Enable middleware to serve swagger-ui (HTML, JS, CSS, etc.), 
            // specifying the Swagger JSON endpoint.
            app.UseSwaggerUI(c =>
            {
                c.SwaggerEndpoint("/swagger/v1/swagger.json", "MyNoSqlServer API V1");
            });            

            app.UseHttpsRedirection();
            app.UseMvc();
            
            app.UseSignalR(routes =>
            {
                routes.MapHub<ChangesHub>("/changes");
            });
            
            
        }
    }
}