# Use .NET 9 SDK (no need for multi-stage for dev)
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS dev
WORKDIR /app

# Copy only the csproj and restore dependencies first (for caching)
COPY AnpCngStations.csproj ./
RUN dotnet restore

# Copy the rest of the app
COPY . .

# Expose port 8080
EXPOSE 8080

# Environment setup for dev mode
ENV ASPNETCORE_ENVIRONMENT=Development
ENV ASPNETCORE_URLS=http://+:8080

# Install dev tools (optional but helps with watch)
RUN dotnet tool install --global dotnet-watch
ENV PATH="$PATH:/root/.dotnet/tools"

# Run the app with live reload
CMD ["dotnet", "watch", "run", "--urls", "http://0.0.0.0:8080"]
