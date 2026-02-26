def update_signing_bh(hostname, domain, logger, config):
    """Marks the signing property on a machine account as being disabled

    Args:
    ----
        hostname(str or list): The machine hostname or list of {"username": hostname, "domain": domain} dicts.
        domain (str): The domain the machine belongs to (if hostname is str).
        logger (Logger): For logging.
        config (ConfigParser): For Bloodhound settings.

    Returns: None
    Raises: ValueError, AuthError, ServiceUnavailable, Exception
    """
    if config.get("BloodHound", "bh_enabled") == "False":
        return
    machines = _normalize_input(hostname, domain)
    uri, driver = _initiate_bloodhound_connection(config)
    try:
        with driver.session().begin_transaction() as tx:
            for machine_info in machines:
                _update_property(machine_info, "smbsigning", False, tx, logger, config)
    except Exception as e:
        _handle_errors(e, uri, config, logger)
    finally:
        driver.close()


def update_ldaps_channel_binding_bh(hostname, domain, logger, config):
    """Marks the ldap channel binding property to false and the ldaps available property to true.

    Args: Similar to above, but for ldap channel binding.
    """
    if config.get("BloodHound", "bh_enabled") == "False":
        return

    machines = _normalize_input(hostname, domain)
    uri, driver = _initiate_bloodhound_connection(config)
    try:
        with driver.session().begin_transaction() as tx:
            for machine_info in machines:
                _update_property(machine_info, "ldapsavailable", True, tx, logger, config)
                _update_property(machine_info, "ldapsepa", False, tx, logger, config)
    except Exception as e:
        _handle_errors(e, uri, config, logger)
    finally:
        driver.close()


def update_ldap_signing_bh(hostname, domain, logger, config):
    """Marks the ldapsigning property to false and the ldap available property to true.

    Args: Similar to above, but for ldap signing.
    """
    if config.get("BloodHound", "bh_enabled") == "False":
        return

    machines = _normalize_input(hostname, domain)
    uri, driver = _initiate_bloodhound_connection(config)
    try:
        with driver.session().begin_transaction() as tx:
            for machine_info in machines:
                _update_property(machine_info, "ldapavailable", True, tx, logger, config)
                _update_property(machine_info, "ldapsigning", False, tx, logger, config)
    except Exception as e:
        _handle_errors(e, uri, config, logger)
    finally:
        driver.close()


def update_web_client_bh(machine_account, domain, logger, config):
    """Marks the webclientrunning property on a machine as True

    Args: Similar to above, but for webclient.
    """
    if config.get("BloodHound", "bh_enabled") == "False":
        return
    machines = _normalize_input(machine_account, domain)
    uri, driver = _initiate_bloodhound_connection(config)
    try:
        with driver.session().begin_transaction() as tx:
            for machine_info in machines:
                _update_property(machine_info, "webclientrunning", True, tx, logger, config)
    except Exception as e:
        _handle_errors(e, uri, config, logger)
    finally:
        driver.close()


def add_user_bh(user, domain, logger, config):
    """Sets owned property on user/machine to True

    Args: Similar, for owned.
    """
    if config.get("BloodHound", "bh_enabled") == "False":
        return
    accounts = _normalize_input(user, domain)
    uri, driver = _initiate_bloodhound_connection(config)
    try:
        with driver.session().begin_transaction() as tx:
            for account_info in accounts:
                _update_property(account_info, "owned", True, tx, logger, config)
    except Exception as e:
        _handle_errors(e, uri, config, logger)
    finally:
        driver.close()


def _normalize_input(input_val, domain):
    """Normalizes str or list input to list of dicts."""
    normalized = []
    if isinstance(input_val, str):
        normalized.append({"username": input_val.upper(), "domain": domain.upper()})
    elif isinstance(input_val, list):
        normalized = input_val  # Assume already dicts
    return normalized


def _update_property(account_info, property_name, value, tx, logger, config):
    """Central func to set property on account node."""
    distinguished_name = "".join([f"DC={dc}," for dc in account_info["domain"].split(".")]).rstrip(",")
    domain_query = _does_domain_exist_in_bloodhound(account_info["domain"], tx)
    domain_val = domain_query[0]["d"].get("name").upper() if domain_query else None
    logger.debug(f"Update property is using domain: {domain_val}. Domain query = {domain_query}. DN = {distinguished_name}")
    if domain_val is None:
        logger.debug(f"Domain {account_info['domain']} not found. Falling back to domainless.")
    _set_property_on_account_node(account_info, domain_val, property_name, value, tx, logger, config)


def _set_property_on_account_node(account_info, domain, property_name, value, tx, logger, config):
    """Central func to update and set property on account node."""
    account_name, account_type = _parse_user_or_machine_account(account_info, domain)
    cypher_match = f"MATCH (c:{account_type} {{name:'{account_name}'}})" if domain else f"MATCH (c:{account_type}) WHERE c.name STARTS WITH '{account_name}'"
    result = tx.run(f"{cypher_match} RETURN c").data()
    if not result:
        logger.fail(f"Account {account_name} not found in BloodHound.")
        return
    if len(result) > 1:
        logger.fail(f"Multiple accounts found for {account_info['username']}. Specify FQDN.")
        return
    current_val = result[0]["c"].get(property_name)
    if current_val != value:
        cypher_set = f"{cypher_match} SET c.{property_name} = {value} RETURN c.name AS name"
        logger.debug(cypher_set)
        result = tx.run(cypher_set).data()[0]
        logger.highlight(f"Node {result['name']} set {property_name} to {value} in BloodHound.")
    else:
        logger.debug(f"The Node {result[0]['c'].get('name', 'Unknown')} already has the {property_name} property set to {value} in BloodHound.")


def _initiate_bloodhound_connection(config):
    """Standardized function for initializing the connection to neo4j."""
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


def _does_domain_exist_in_bloodhound(domain_name, tx):
    """A basic query query used for identifying if a domain exists in bloodhound already"""
    return tx.run(f"MATCH (d:Domain) WHERE d.name = '{domain_name}' RETURN d").data()


def _parse_user_or_machine_account(user_info, domain=None):
    """Standardized function for parsing identifying the type of account the code is dealing with"""
    user_owned = ""
    account_type = ""
    if user_info["username"][-1] == "$":
        user_owned = f"{user_info['username'][:-1]}.{domain}" if domain is not None else user_info["username"][:-1]
        account_type = "Computer"

    else:
        user_owned = f"{user_info['username']}@{domain}" if domain is not None else user_info["username"]
        account_type = "User"

    return user_owned, account_type


def _handle_errors(e, uri, config, logger):
    from neo4j.exceptions import AuthError, ServiceUnavailable

    if isinstance(e, ValueError):
        logger.fail(f"Inputted account is not a machine account: {e!s}")
    elif isinstance(e, AuthError):
        logger.fail(f"Invalid Neo4j creds: {config.get('BloodHound', 'bh_user')}:{config.get('BloodHound', 'bh_pass')}")
    elif isinstance(e, ServiceUnavailable):
        logger.fail(f"Neo4j unavailable on {uri}")
    else:
        logger.fail(f"Unexpected Neo4j error: {e}")
