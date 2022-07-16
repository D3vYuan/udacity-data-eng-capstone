import configparser

def load_config(config_file="config.ini"):
    """
    - Load the config file
    - Returns the parameters of the config file
    """
    
    config = configparser.ConfigParser()
    try:
        config.read(config_file)
        return config
    except Exception as e:
        print(f"{config_file} not loaded due to {e}")

    return None

def main():
    """
    - Returns the database configuration
    """
    config = load_config()
    return config


if __name__ == "__main__":
    main()