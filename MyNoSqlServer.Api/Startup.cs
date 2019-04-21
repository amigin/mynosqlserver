﻿using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MyNoSqlServer.Api.Hubs;
using MyNoSqlServer.Api.Services;
using MyNoSqlServer.AzureStorage;
using MyNoSqlServer.Domains;


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
            
            services.AddMvc(o =>
                {
                    o.EnableEndpointRouting = false;
                })
                .AddNewtonsoftJson();
            
            /*
            services.Configure<ForwardedHeadersOptions>(options =>
            {
                options.ForwardedHeaders = 
                    ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto;
            });

            */
            
            services.AddSignalR(); 
            
            
            services.AddSwaggerDocument();

            // Register the Swagger generator, defining 1 or more Swagger documents
   //         services.AddSwaggerGen(c =>
   //         {
   //             c.SwaggerDoc("v1", new OpenApiInfo
    //            {
    //                Title = "MyNoSql API",
    //                Version = "1.0"

//                });
 //           });

            
            var settings = SettingsLoader.LoadSettings();
            
            settings.BackupAzureConnectString.BindAzureStorage();

            ServiceLocator.Synchronizer.ChangesPublisher = new ChangesPublisherToSignalR();

        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app)
        {
            
            // Enable middleware to serve generated Swagger as a JSON endpoint.
     //      app.UseSwagger();

            // Enable middleware to serve swagger-ui (HTML, JS, CSS, etc.), 
            // specifying the Swagger JSON endpoint.
    //       app.UseSwaggerUI(c =>
    //        {
     //           c.SwaggerEndpoint("/swagger/v1/swagger.json", "MyNoSqlServer API V1");
     //       });            

            //app.UseHttpsRedirection();
            
            app.Use((context, next) =>
            {
                if (context.Request.Headers.ContainsKey("X-Forwarded-Proto"))
                    context.Request.Scheme = context.Request.Headers["X-Forwarded-Proto"];
                return next();
            });
            
          //  app.UseForwardedHeaders();

            
            app.UseStaticFiles();
            
                 app.UseSwagger();
                app.UseSwaggerUi3();
            
            app.UseMvc();
            
            

            
            app.UseSignalR(routes =>
            {
                routes.MapHub<ChangesHub>("/changes");
            });
            
            

            
        }
    }
}