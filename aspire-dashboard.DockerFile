FROM mcr.microsoft.com/dotnet/aspire-dashboard:9.0

ENV DOTNET_DASHBOARD_UNSECURED_ALLOW_ANONYMOUS=true

## Specify a directory for persistent key storage
#ENV DOTNET_DASHBOARD_DATAPROTECTION_KEYDIRECTORY=/var/aspire-dashboard-keys
#
## Create the key directory
#RUN mkdir -p /var/aspire-dashboard-keys

EXPOSE 18888 18889

CMD ["dotnet", "Microsoft.DotNet.Aspire.Dashboard.dll"]
