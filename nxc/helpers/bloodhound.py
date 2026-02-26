def mark_web_client_enabled(machine_account, domain, logger, config):
    """Marks the web client service as being enabled on a machine account in bloodhound

    Args:
    ----
        machine_account(str or list): The machine account/username or a list of machine account/username dictioniares.
        domain (str): The domain the machine account belongs to.
        logger (Logger): The logger object for logging messages.
        config (ConfigParser): The configurationb object for accessing Bloodhound Settings

    Returns:
    -------
        None

    Raises:
    ------
        ValueError: If the inputted account is normal user instead of a machine account
        AuthError: If the provided Neo4J credentials are not valid.
        ServiceUnavailable: If Neo4J is not available on the specified URI.
        Exception: If an unexpected error occurs with Neo4J.
    """
    machines_where_webclient_is_enabled = []
    if isinstance(machine_account, str):
        machines_where_webclient_is_enabled.append({"username": machine_account.upper(), "domain": domain.upper()})
    elif isinstance(machine_account, list):
        machines_where_webclient_is_enabled = machine_account

    if config.get("BloodHound", "bh_enabled") != "False":
        from neo4j.exceptions import AuthError, ServiceUnavailable

        uri, driver = _initiate_bloodhound_connection(config)
        try:
            with driver.session().begin_transaction() as tx:
                for machine_info in machines_where_webclient_is_enabled:
                    distinguished_name = "".join([f"DC={dc}," for dc in machine_info["domain"].split(".")]).rstrip(",")
                    domain_query = _does_domain_exist_in_bloodhound(distinguished_name, tx)
                    if not domain_query:
                        logger.debug(f"Domain {machine_info['domain']} not found in BloodHound. Falling back to domainless query.")
                        _adjust_webclient_property_without_domain(machine_info, tx, logger)
                    else:
                        domain = domain_query[0]["d"].get("name")
                        _adjust_webclient_property_with_domain(machine_info, domain, tx, logger)
        except ValueError:
            logger.fail(f"Inputted account ({machine_info['username']}) is not a machine account")
        except AuthError:
            logger.fail(f"Provided Neo4J credentials ({config.get('BloodHound', 'bh_user')}:{config.get('BloodHound', 'bh_pass')}) are not valid.")
        except ServiceUnavailable:
            logger.fail(f"Neo4J does not seem to be available on {uri}.")
        except Exception as e:
            logger.fail(f"Unexpected error with Neo4J: {e}")
        finally:
            driver.close()


def add_user_bh(user, domain, logger, config):
    """Adds a user to the BloodHound graph database.

    Args:
    ----
        user (str or list): The username of the user or a list of user dictionaries.
        domain (str): The domain of the user.
        logger (Logger): The logger object for logging messages.
        config (ConfigParser): The configuration object for accessing BloodHound settings.

    Returns:
    -------
        None

    Raises:
    ------
        AuthError: If the provided Neo4J credentials are not valid.
        ServiceUnavailable: If Neo4J is not available on the specified URI.
        Exception: If an unexpected error occurs with Neo4J.
    """
    users_owned = []
    if isinstance(user, str):
        users_owned.append({"username": user.upper(), "domain": domain.upper()})
    else:
        users_owned = user

    if config.get("BloodHound", "bh_enabled") != "False":
        # we do a conditional import here to avoid loading these if BH isn't enabled
        from neo4j.exceptions import AuthError, ServiceUnavailable

        uri, driver = _initiate_bloodhound_connection(config)

        try:
            with driver.session().begin_transaction() as tx:
                for user_info in users_owned:
                    distinguished_name = "".join([f"DC={dc}," for dc in user_info["domain"].split(".")]).rstrip(",")
                    domain_query = _does_domain_exist_in_bloodhound(distinguished_name, tx)
                    if not domain_query:
                        logger.debug(f"Domain {user_info['domain']} not found in BloodHound. Falling back to domainless query.")
                        _add_without_domain(user_info, tx, logger)
                    else:
                        domain = domain_query[0]["d"].get("name")
                        _add_with_domain(user_info, domain, tx, logger)
        except AuthError:
            logger.fail(f"Provided Neo4J credentials ({config.get('BloodHound', 'bh_user')}:{config.get('BloodHound', 'bh_pass')}) are not valid.")
        except ServiceUnavailable:
            logger.fail(f"Neo4J does not seem to be available on {uri}.")
        except Exception as e:
            logger.fail(f"Unexpected error with Neo4J: {e}")
        finally:
            driver.close()


def _initiate_bloodhound_connection(config):
    from neo4j import GraphDatabase

    uri = f"bolt://{config.get('BloodHound', 'bh_uri')}:{config.get('BloodHound', 'bh_port')}"
    driver = GraphDatabase.driver(
        uri,
        auth=(
            config.get("BloodHound", "bh_user"),
            config.get("BloodHound", "bh_pass"),
        ),
        encrypted=False,
    )

    return uri, driver


def _does_domain_exist_in_bloodhound(distinguished_name, tx):
    return tx.run(f"MATCH (d:Domain) WHERE d.distinguishedname STARTS WITH '{distinguished_name}' RETURN d").data()


def _adjust_webclient_property_with_domain(machine_info, domain, tx, logger):
    webclient_enabled_machine, account_type = _parse_user_or_machine_account(machine_info, domain)

    result = tx.run(f"MATCH (c:{account_type} {{name:'{webclient_enabled_machine}'}}) RETURN c").data()

    if len(result) == 0:
        logger.fail("Computer not found in the BloodHound database")
        return
    if result[0]["c"].get("webclientrunning") in (False, None):
        logger.debug(f"MATCH (c:{account_type} {{name:'{webclient_enabled_machine}'}}) SET c.webclientrunning=True RETURN c.name AS name")
        result = tx.run(f"MATCH (c:{account_type} {{name:'{webclient_enabled_machine}'}}) SET c.webclientrunning=True RETURN c.name AS name").data()[0]
        logger.highlight(f"Node {result['name']} successfully noted that webclient was running on this device in the BloodHound database")


def _adjust_webclient_property_without_domain(machine_info, tx, logger):
    webclient_enabled_machine, account_type = _parse_user_or_machine_account(machine_info)

    result = tx.run(f"MATCH (c:{account_type}) WHERE c.name STARTS WITH '{webclient_enabled_machine}' RETURN c").data()

    if len(result) == 0:
        logger.fail("Account not found in the BloodHound database.")
        return
    elif len(result) >= 2:
        logger.fail(f"Multiple accounts found with the name '{webclient_enabled_machine['username']}' in the BloodHound database. Please specify the FQDN ex:domain.local")
        return
    elif result[0]["c"].get("webclientrunning") in (False, None):
        logger.debug(f"MATCH (c:{account_type} {{name:'{result[0]['c']['name']}'}}) SET c.webclientrunning=True RETURN c.name AS name")
        result = tx.run(f"MATCH (c:{account_type} {{name:'{result[0]['c']['name']}'}}) SET c.webclientrunning=True RETURN c.name AS name").data()[0]
        logger.highlight(f"Node {result['name']} successfully noted that webclient was running on this device in the BloodHound database")


def _parse_user_or_machine_account(user_info, domain=None):
    user_owned = ""
    account_type = ""
    if user_info["username"][-1] == "$":
        user_owned = f"{user_info['username'][:-1]}.{domain}" if domain is not None else user_info["username"][:-1]
        account_type = "Computer"

    else:
        user_owned = f"{user_info['username']}@{domain}" if domain is not None else user_info["username"]
        account_type = "User"

    return user_owned, account_type


def _add_with_domain(user_info, domain, tx, logger):
    user_owned, account_type = _parse_user_or_machine_account(user_info, domain)

    result = tx.run(f"MATCH (c:{account_type} {{name:'{user_owned}'}}) RETURN c").data()

    if len(result) == 0:
        logger.fail("Account not found in the BloodHound database.")
        return
    if result[0]["c"].get("owned") in (False, None):
        logger.debug(f"MATCH (c:{account_type} {{name:'{user_owned}'}}) SET c.owned=True RETURN c.name AS name")
        result = tx.run(f"MATCH (c:{account_type} {{name:'{user_owned}'}}) SET c.owned=True RETURN c.name AS name").data()[0]
        logger.highlight(f"Node {result['name']} successfully set as owned in BloodHound")


def _add_without_domain(user_info, tx, logger):
    user_owned, account_type = _parse_user_or_machine_account(user_info)

    result = tx.run(f"MATCH (c:{account_type}) WHERE c.name STARTS WITH '{user_owned}' RETURN c").data()

    if len(result) == 0:
        logger.fail("Account not found in the BloodHound database.")
        return
    elif len(result) >= 2:
        logger.fail(f"Multiple accounts found with the name '{user_info['username']}' in the BloodHound database. Please specify the FQDN ex:domain.local")
        return
    elif result[0]["c"].get("owned") in (False, None):
        logger.debug(f"MATCH (c:{account_type} {{name:'{result[0]['c']['name']}'}}) SET c.owned=True RETURN c.name AS name")
        result = tx.run(f"MATCH (c:{account_type} {{name:'{result[0]['c']['name']}'}}) SET c.owned=True RETURN c.name AS name").data()[0]
        logger.highlight(f"Node {result['name']} successfully set as owned in BloodHound")
