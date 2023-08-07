# b24_loader — easy way to save data from Bitrix24 to database.

This product provides work with MySQL, PostgreSQL and Clickhouse databases.
This README provides instructions for setting up and running the project.


## Instructions

1. Clone the repository to your local machine:
   ```
   git clone https://github.com/your-username/your-repository.git
   ```

2. Copy the `config.example.yaml` file and rename it to `config.yaml`. Open the config.yaml file and fill it with your own data.

3. Download Docker from the official website: [https://www.docker.com/get-started](https://www.docker.com/get-started)

4. To build and run the Docker image, use the following commands:

   - Build the Docker image:
     ```
     docker build -t your-image-name .
     ```

   - Run the Docker container:
     ```
     docker run -d -p your-port:container-port your-image-name
     ```

   Replace your-image-name with the desired name for your Docker image, your-port with the desired port to access the container, and container-port with the port on which your application runs inside the container.

   For example, if your application runs on port 3000 and you want to access it on port 8080 on your local machine, use:
   ```
   docker run -d -p 8080:3000 your-image-name
   ```

   Make sure to map the correct ports based on your application configuration.

## Cloud Function generation

All files for Yandex Cloud Functions are automatically generated in the `/cf` directory of the project.

**When deploying the function and connecting the outgoing webhook from Bitrix24, be sure to write the `application id` value to the environment variable `app_token`.**

## Additional Notes

- Modify the code in the repository according to your needs.

- For any additional dependencies or requirements, refer to the project documentation or the source code.

- You can also add any other relevant information or instructions in this README file as needed.

- If you encounter any issues or have questions, please refer to the project's issue tracker on GitHub for assistance.

## License

<a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work "<span xmlns:dct="http://purl.org/dc/terms/" href="http://purl.org/dc/dcmitype/InteractiveResource" property="dct:title" rel="dct:type">Loader for Bitrix24</span>" created by <a xmlns:cc="http://creativecommons.org/ns#" href="https://polyakov.marketing/" property="cc:attributionName" rel="cc:attributionURL">Aleksandr Poliakov</a> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.<br />Based on a work at <a xmlns:dct="http://purl.org/dc/terms/" href="https://github.com/artwist-polyakov/Bitrix24_loader" rel="dct:source">https://github.com/artwist-polyakov/Bitrix24_loader</a>.<br />Permissions beyond the scope of this license may be available at <a xmlns:cc="http://creativecommons.org/ns#" href="https://polyakov.marketing/" rel="cc:morePermissions">https://polyakov.marketing/</a>.
