FROM mcr.microsoft.com/dotnet/aspire-dashboard:8.1.0

ENV DOTNET_DASHBOARD_UNSECURED_ALLOW_ANONYMOUS=true

EXPOSE 18888 18889

CMD ["dotnet", "Microsoft.DotNet.Aspire.Dashboard.dll"]
