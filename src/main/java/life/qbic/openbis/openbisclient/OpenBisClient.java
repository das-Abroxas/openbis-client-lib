package life.qbic.openbis.openbisclient;

import static life.qbic.openbis.openbisclient.helper.OpenBisClientHelper.*;

import ch.ethz.sis.openbis.generic.asapi.v3.IApplicationServerApi;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.attachment.Attachment;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.common.interfaces.IEntityType;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.common.search.SearchResult;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.dataset.DataSet;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.dataset.search.DataSetSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.entitytype.id.EntityTypePermId;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.Experiment;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.ExperimentType;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.create.ExperimentCreation;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.fetchoptions.ExperimentFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.id.ExperimentIdentifier;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.id.ExperimentPermId;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.search.ExperimentSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.search.ExperimentTypeSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.person.Person;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.person.search.PersonSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.project.Project;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.project.create.ProjectCreation;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.project.fetchoptions.ProjectFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.project.id.ProjectIdentifier;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.project.id.ProjectPermId;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.project.search.ProjectSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.property.PropertyAssignment;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.property.PropertyType;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.property.search.PropertyAssignmentSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.property.search.PropertyTypeSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.roleassignment.Role;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.roleassignment.RoleAssignment;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.roleassignment.RoleLevel;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.roleassignment.search.RoleAssignmentSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.Sample;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.SampleType;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.create.SampleCreation;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.fetchoptions.SampleFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.id.SampleIdentifier;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.id.SamplePermId;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.search.SampleSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.search.SampleTypeSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.service.CustomASServiceExecutionOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.service.id.CustomASServiceCode;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.space.Space;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.space.create.SpaceCreation;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.space.fetchoptions.SpaceFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.space.id.SpacePermId;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.space.search.SpaceSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.vocabulary.Vocabulary;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.vocabulary.VocabularyTerm;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.vocabulary.fetchoptions.VocabularyTermFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.vocabulary.search.VocabularySearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.vocabulary.search.VocabularyTermSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.exceptions.NotFetchedException;
import ch.ethz.sis.openbis.generic.dssapi.v3.IDataStoreServerApi;
import ch.systemsx.cisd.common.exceptions.UserFailureException;
import ch.systemsx.cisd.common.spring.HttpInvokerUtils;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The type Open bis client.
 */
public class OpenBisClient implements IOpenBisClient {

  private static final Logger logger = LogManager.getLogger(OpenBisClient.class);
  private final int TIMEOUT = 100000;
  private final String userId, password, serverUrl, asApiUrl, dsApiUrl;
  private final IApplicationServerApi v3;
  private final IDataStoreServerApi dss3;
  private String sessionToken;


  /**
   * Instantiates a new opnBIS client.
   *
   * @param userId The user id
   * @param password The password
   * @param serverURL The server url
   */
  public OpenBisClient(String userId, String password, String serverURL) {
    this.userId = userId;
    this.password = password;
    this.serverUrl = serverURL;
    this.asApiUrl = serverURL + "/openbis/openbis"+ IApplicationServerApi.SERVICE_URL;
    this.dsApiUrl = serverURL + "/datastore" + IDataStoreServerApi.SERVICE_URL;

    // Get a reference to Application Server API and Datastore Server API
    v3 = HttpInvokerUtils.createServiceStub(IApplicationServerApi.class, asApiUrl, TIMEOUT);
    dss3 = HttpInvokerUtils.createServiceStub(IDataStoreServerApi.class, dsApiUrl, TIMEOUT);
    sessionToken = null;
  }

  /**
   * Function map an integer value to a char
   *
   * @param i The integer value which should be mapped
   * @return the resulting char value
   */
  public static char mapToChar(int i) {
    i += 48;
    if (i > 57) {
      i += 7;
    }
    return (char) i;
  }

  /**
   * Function to generate the checksum for the given barcode string
   *
   * @param s the barcode string
   * @return the checksum for the given barcode
   */
  public static char checksum(String s) {
    int i = 1;
    int sum = 0;
    for (int idx = 0; idx <= s.length() - 1; idx++) {
      sum += (((int) s.charAt(idx))) * i;
      i += 1;
    }
    return mapToChar(sum % 34);
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Getter ----------------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Returns the v3 ApplicationServerApi object.
   *
   * @return openBIS v3 ApplicationServerApi
   */
  public IApplicationServerApi getV3() {
    // ToDo: Should be removed so all API calls stay inside this class.
    return v3;
  }

  /**
   * Get session token of current openBIS session
   *
   * @return session token as string
   */
  @Override
  public String getSessionToken() {
    // ToDo: Should be removed. Session token is nobody's business outside this class.
    return sessionToken;
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- login / logout  -------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Logs in to the openBIS server with the credentials provided at instantiation of this class.
   * Also invalidates the current session token if present.
   */
  @Override
  public void login() {
    if (loggedIn()) {
      logout();
    }
    // Login to obtain a session token
    sessionToken = v3.login(userId, password);
  }

  /**
   * Logs in to the openBIS server as the user of the provided username.
   * A user with the provided username has to exist in the openBIS server.
   * Also invalidates the current session token if present.
   * @param user openBIS user name
   */
  public void loginAsUser(String user) {
    if (loggedIn()) {
      logout();
    }

    // Login to obtain a session token
    sessionToken = v3.loginAs(userId, password, user);
  }

  /**
   * Checks if issued session token is still active in the openBIS instance.
   * Session tokens are issued with {@link #login() login} and {@link #loginAsUser(String) loginAsUser}.
   * @return True if session token is still active else false
   */
  @Override
  public boolean loggedIn() {
    try {
      return v3.isSessionActive(sessionToken);
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Checks if session token for provided openBIS user is still active in the openBIS instance.
   * Session tokens are issued with {@link #login() login} and {@link #loginAsUser(String) loginAsUser}.
   * @param user openBIS user name
   * @return True if session token of user is still active else false
   */
  public boolean loggedIn(String user) {
    try {
      return v3.isSessionActive(sessionToken) && v3.getSessionInformation(sessionToken).getUserName().equals(user);
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Checks if issued session token is still active in the openBIS instance.
   * If not, reconnection to the openBIS server is attempted once with the credentials
   *   provided at instantiation of this class.
   */
  @Override
  public void ensureLoggedIn() {
    if (!this.loggedIn()) {
      this.login();
    }
  }

  /**
   * Checks if session token for provided openBIS user is still active in the openBIS instance.
   * If not, login to the openBIS server as the provided user is attempted once.
   */
  public void ensureLoggedIn(String user) {
    if (!this.loggedIn(user)) {
      this.loginAsUser(user);
    }
  }

  /**
   * Logs out of the openBIS server and invalidates the issued session token.
   */
  @Override
  public void logout() {
    if (loggedIn()) {
      v3.logout(sessionToken);
    }
    sessionToken = null;
  }

  /**
   * Returns whether a user is instance admin in the openBIS instance.
   * @param user openBIS user name
   * @return True if user is instance admin false otherwise
   * @throws IllegalArgumentException If no user with the provided name could be found.
   */
  @Override
  public boolean isUserAdmin(String user) throws IllegalArgumentException {
    ensureLoggedIn();

    try {
      PersonSearchCriteria psc = new PersonSearchCriteria();
      psc.withUserId().thatEquals(user);

      SearchResult<Person> person = v3.searchPersons(sessionToken, psc, fetchPersonCompletely());

      if (person.getObjects().isEmpty()) {
        throw new IllegalArgumentException("Could not find openBIS user with id: " + user);
      }

      for (RoleAssignment role : person.getObjects().get(0).getRoleAssignments()) {
        // Only INSTANCE_ADMIN are accepted as openBIS admins
        if (role.getRoleLevel().equals(RoleLevel.INSTANCE) && role.getRole().equals(Role.ADMIN)) {
          return true;
        }
      }
      return false;

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch persons. Is currently logged in user admin? Returned false.");
      return false;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Spaces ----------------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Get all Space codes which are registered in this openBIS instance.
   * @return List with the codes of all available spaces
   */
  @Override
  public List<String> listSpaces() {
    ensureLoggedIn();

    try {
      List<String> spaceCodes = new ArrayList<>();
      SearchResult<Space> spaces = v3.searchSpaces(sessionToken, new SpaceSearchCriteria(), new SpaceFetchOptions());

      if (spaces.getTotalCount() == 0)
        logger.info("No spaces found with listSpaces().");

      for (Space space : spaces.getObjects()) {
        spaceCodes.add(space.getCode());
      }

      return spaceCodes;

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch spaces. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn("listSpaces() returned null.");
      return null;
    }
  }

  /**
   * Get all Space codes which are available for the provided openBIS user.
   * @param user openBIS user name
   * @return List with codes of all available spaces for specific openBIS user
   */
  @Override
  public List<String> getUserSpaces(String userID) {
    // this sets the user sessionToken
    loginAsUser(userID);
    List<String> spaceIdentifiers = new ArrayList<>();
    // we are not using external functions to make sure this user is actually used
    try {
      SearchResult<Space> spaces =
              v3.searchSpaces(sessionToken, new SpaceSearchCriteria(), new SpaceFetchOptions());
      if (spaces != null) {
        for (Space space : spaces.getObjects()) {
          spaceIdentifiers.add(space.getCode());
        }
      }
    } catch (UserFailureException u) {
      logger.error("Could not fetch spaces for user " + userID
              + ", because they could not be logged in. Is user " + this.userId + " an admin user?");
      logger.warn("No spaces were returned.");
    }
    logout();
    login();

    return spaceIdentifiers;
  }

  /**
   * Get all Space codes which are available for the provided openBIS user.
   * @param user openBIS user name
   * @return List with codes of all available spaces for specific openBIS user
   */
  public List<String> listSpacesForUser(String user) {
    ensureLoggedIn(user);

    try {
      SearchResult<Space> spaces = v3.searchSpaces(sessionToken, new SpaceSearchCriteria(), fetchSpacesCompletely());

      if (spaces.getTotalCount() == 0)
        logger.info(String.format("No spaces found with listSpacesForUser(\"%s\").", user));

      List<String> spaceIdentifiers = spaces.getObjects().stream().map(Space::getCode).collect(Collectors.toList());

      return spaceIdentifiers;

    } catch (UserFailureException ufe) {
      logger.error(String.format("Could not fetch spaces of user %s. Does this user exist in openBIS?", user));
      logger.warn(String.format("listSpacesForUser(\"%s\") returned null.", user));
      return null;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Projects --------------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Get all projects which are registered in this openBIS instance.
   * @return List with all available projects in this openBIS instance
   */
  @Override
  public List<Project> listProjects() {
    ensureLoggedIn();

    try {
      SearchResult<Project> projects = v3.searchProjects(sessionToken, new ProjectSearchCriteria(), fetchProjectsCompletely());

      if (projects.getTotalCount() == 0)
        logger.info("No projects found with listProjects().");

      return projects.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch projects. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn("listProjects() returned null.");
      return null;
    }
  }

  /**
   * Get all projects which are available to the provided user in this openBIS instance.
   * @param user openBIS user name
   * @return List with all available projects for specific openBIS user
   */
  public List<Project> listProjectsForUser(String user) {
    ensureLoggedIn(user);

    try {
      SearchResult<Project> projects = v3.searchProjects(sessionToken, new ProjectSearchCriteria(), fetchProjectsCompletely());

      if (projects.getTotalCount() == 0)
        logger.info(String.format("No projects found with listProjectsForUser(\"%s\").", user));

      return projects.getObjects();

    } catch (UserFailureException ufe) {
      logger.error(String.format("Could not fetch projects of user %s. Does this user exist in openBIS?", user));
      logger.warn(String.format("listProjectsForUser(\"%s\") returned null.", user));
      return null;
    }
  }

  /**
   * Get all projects registered under the provided Space code.
   * @param spaceCode Code of the openBIS space
   * @return List of openBIS v3 Project
   */
  @Override
  public List<Project> getProjectsOfSpace(String spaceCode) {
    ensureLoggedIn();

    try {
      ProjectSearchCriteria psc = new ProjectSearchCriteria();
      psc.withSpace().withCode().thatEquals(spaceCode);

      SearchResult<Project> projects = v3.searchProjects(sessionToken, psc, fetchProjectsCompletely());

      if (projects.getTotalCount() == 0)
        logger.info(String.format("No projects found with getProjectsOfSpace(\"%s\").", spaceCode));

      return projects.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch projects of space. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getProjectsOfSpace(\"%s\") returned null.", spaceCode));
      return null;
    }
  }

  /**
   * Get a map with all project codes as key and their experiments as value which are registered under the provided Space code.
   * @param spaceIdentifier Code of the openBIS space
   * @return Map with project code as key and its experiments as value
   */
  @Override
  public Map<String, List<Experiment>> getProjectExperimentMapping(String spaceIdentifier) {
    // ToDo: Is this necessary? With getProjectsOfSpace(String) you also get all projects with their experiments.
    //       With the correct ProjectFetchOptions the experiments are also fetched completely.
    ensureLoggedIn();

    Map<String, List<Experiment>> projectExperimentMapping = new HashMap<>();
    List<Project> projects = getProjectsOfSpace(spaceIdentifier);

    if (projects == null || projects.isEmpty()) { return projectExperimentMapping; }

    for (Project project : projects) {
      String code = project.getCode();
      List<Experiment> experiments = getExperimentsOfProjectByCode(code);

      projectExperimentMapping.put(code, experiments);
    }

    return projectExperimentMapping;
  }

  /**
   * Get the project under which the experiment of the provided identifier is registered.
   * @param experimentIdentifier Identifier of the openBIS experiment
   * @return openBIS v3 Project
   */
  @Override
  public Project getProjectOfExperimentByIdentifier(String experimentIdentifier) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria esc = new ExperimentSearchCriteria();
      esc.withId().thatEquals(new ExperimentIdentifier(experimentIdentifier));

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, esc, fetchExperimentsCompletely());

      if (experiments.getTotalCount() == 0)
        logger.info(String.format("No experiments found with getProjectOfExperimentByIdentifier(\"%s\").", experimentIdentifier));

      return experiments.getObjects().isEmpty() ? null : experiments.getObjects().get(0).getProject();

    } catch (NotFetchedException nfe) {
      logger.error("Experiment has no fetched project.");
      logger.warn("getProjectOfExperimentByIdentifier(String experimentIdentifier) returned null.");
      return null;

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getProjectOfExperimentByIdentifier(\"%s\") returned null.", experimentIdentifier));
      return null;
    }
  }

  /**
   * Get the project of the provided project code or identifier.
   * @param projectCodeOrIdentifier Code or identifier of the openBIS project
   * @return openBIS v3 Project
   */
  public Project getProject(String projectCodeOrIdentifier) {
    ensureLoggedIn();

    try {
      ProjectSearchCriteria psc = new ProjectSearchCriteria();
      psc.withOrOperator();
      psc.withId().thatEquals(new ProjectIdentifier(projectCodeOrIdentifier));
      psc.withCode().thatEquals(projectCodeOrIdentifier);

      SearchResult<Project> projects = v3.searchProjects(sessionToken, psc, fetchProjectsCompletely());

      if (projects.getTotalCount() == 0)
        logger.info(String.format("No projects found with getProject(\"%s\").", projectCodeOrIdentifier));

      return projects.getObjects().isEmpty() ? null : projects.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch projects. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getProject(\"%s\") returned null.", projectCodeOrIdentifier));
      return null;
    }
  }

  /**
   * Get the project of the provided project identifier.
   * @param projectIdentifier Identifier of the openBIS project
   * @return openBIS v3 Project
   */
  @Override
  public Project getProjectByIdentifier(String projectIdentifier) {
    // ToDo: Can be removed from IOpenBisClient as getProject(String) is sufficient
    return getProject(projectIdentifier);  // ensureLoggedIn() is called in getProject
  }

  /**
   * Get the project of the provided project code.
   * @param projectCode Identifier of the openBIS project
   * @return openBIS v3 Project
   */
  @Override
  public Project getProjectByCode(String projectCode) {
    // ToDo: Can be removed from IOpenBisClient as getProject(String) is sufficient
    return getProject(projectCode);  // ensureLoggedIn() is called in getProject
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Experiment / ExperimentType -------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Get all Experiments which are registered in the openBIS instance.
   * @return List of openBIS v3 Experiment
   */
  @Override
  public List<Experiment> listExperiments() {
    ensureLoggedIn();

    try {
      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, new ExperimentSearchCriteria(), fetchExperimentsCompletely());

      if (experiments.getTotalCount() == 0)
        logger.info("No experiments found with listExperiments().");

      return experiments.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn("listExperiments() returned null.");
      return null;
    }
  }

  /**
   * Get all Experiments which are available to the provided user in the openBIS instance
   * @param user openBIS user name
   * @return List of openBIS v3 Experiment
   */
  public List<Experiment> listExperimentsForUser(String user) {
    ensureLoggedIn(user);

    try {
      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, new ExperimentSearchCriteria(), fetchExperimentsCompletely());

      if (experiments.getTotalCount() == 0)
        logger.info(String.format("No experiments found with listExperimentsForUser(\"%s\").", user));

      return experiments.getObjects();

    } catch (UserFailureException ufe) {
      logger.error(String.format("Could not fetch experiments of user %s. Does this user exist in openBIS?", user));
      logger.warn(String.format("listExperimentsForUser(\"%s\") returned null.", user));
      return null;
    }
  }

  /**
   * Get all Experiments which are available to the provided user in the openBIS instance
   * @param userID openBIS user name
   * @return List of openBIS v3 Experiment
   */
  @Override
  public List<Experiment> getExperimentsForUser(String userID) {
    loginAsUser(userID);
    // we are not reusing other functions to be sure the user in question is actually used
    SearchResult<Experiment> experiments = null;
    try {
      experiments = v3.searchExperiments(sessionToken, new ExperimentSearchCriteria(),
              fetchExperimentsCompletely());
    } catch (UserFailureException u) {
      logger.error("Could not fetch experiments for user " + userID
              + ", because they could not be logged in. Is user " + this.userId + " an admin user?");
      logger.warn("No experiments were returned.");
    }
    logout();
    login();
    if (experiments == null || experiments.getObjects().isEmpty()) {
      return null;
    } else {
      return experiments.getObjects();
    }
  }

  /**
   * Get all Experiments which are registered under the provided space code.
   * @param spaceCode Code of the openBIS space
   * @return List of openBIS v3 Experiment
   */
  @Override
  public List<Experiment> getExperimentsOfSpace(String spaceCode) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria esc = new ExperimentSearchCriteria();
      esc.withProject().withSpace().withCode().thatEquals(spaceCode);

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, esc, fetchExperimentsCompletely());

      if (experiments.getTotalCount() == 0)
        logger.info(String.format("No experiments found with getExperimentsOfSpace(\"%s\").", spaceCode));

      return experiments.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getExperimentsOfSpace(\"%s\") returned null.", spaceCode));
      return null;
    }
  }

  /**
   * Get all experiments registered under the provided openBIS project.
   * @param project Project that exists in the openBIS instance
   * @return List of openBIS v3 Experiment
   */
  @Override
  public List<Experiment> getExperimentsForProject(Project project) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria esc = new ExperimentSearchCriteria();
      esc.withProject().withCode().thatEquals(project.getCode());

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, esc, fetchExperimentsCompletely());

      if (experiments.getTotalCount() == 0)
        logger.info(String.format("No experiments found with getExperimentsForProject(%s).", project.toString()));

      return experiments.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getExperimentsForProject(\"%s\") returned null.", project.getCode()));
      return null;
    }
  }

  /**
   * Get all experiments which are registered under the provided project code or identifier.
   * @param projectCodeOrIdentifier Code or identifier of the openBIS project
   * @return List of openBIS v3 Experiment
   */
  @Override
  public List<Experiment> getExperimentsForProject(String projectCodeOrIdentifier) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria esc = new ExperimentSearchCriteria();
      esc.withOrOperator();
      esc.withProject().withCode().thatEquals(projectCodeOrIdentifier);
      esc.withProject().withId().thatEquals(new ProjectIdentifier(projectCodeOrIdentifier));

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, esc, fetchExperimentsCompletely());

      if (experiments.getTotalCount() == 0)
        logger.info(String.format("No experiments found with getExperimentsForProject(%s).", projectCodeOrIdentifier));

      return experiments.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getExperimentsForProject(\"%s\") returned null.", projectCodeOrIdentifier));
      return null;
    }
  }

  /**
   * Get all experiments which are registered under the provided project identifier.
   * @param projectIdentifier Identifier of the openBIS project
   * @return List of openBIS v3 Experiment
   */
  @Override
  public List<Experiment> getExperimentsOfProjectByIdentifier(String projectIdentifier) {
    // ToDo: Can be removed from IOpenBisClient as getExperimentsForProject(String) is sufficient
    return getExperimentsForProject(projectIdentifier);  // ensureLoggedIn() is called in getExperimentsForProject
  }

  /**
   * Get all experiments which are registered under the provided project code.
   * @param projectCode Code of the openBIS project
   * @return List of openBIS v3 Experiment
   */
  @Override
  public List<Experiment> getExperimentsOfProjectByCode(String projectCode) {
    // ToDo: Can be removed from IOpenBisClient as getExperimentsForProject(String) is sufficient
    return getExperimentsForProject(projectCode);  // ensureLoggedIn() is called in getExperimentsForProject
  }

  /**
   * Get experiment of the provided experiment code or identifier.
   * @param experimentCodeOrIdentifier Code or identifier of the openBIS experiment
   * @return openBIS v3 Experiment
   */
  public Experiment getExperiment(String experimentCodeOrIdentifier) {
    if (experimentCodeOrIdentifier.startsWith("/"))
      return getExperimentById(experimentCodeOrIdentifier);  // ensureLoggedIn() is called inside
    else
      return getExperimentByCode(experimentCodeOrIdentifier);  // ensureLoggedIn() is called inside
  }

  /**
   * Get experiment of the provided experiment code.
   * @param experimentCode Code of the openBIS experiment
   * @return openBIS v3 Experiment
   */
  @Override
  public Experiment getExperimentByCode(String experimentCode) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria esc = new ExperimentSearchCriteria();
      esc.withCode().thatEquals(experimentCode);

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, esc, fetchExperimentsCompletely());

      if (experiments.getTotalCount() == 0)
        logger.info(String.format("No experiments found with getExperimentByCode(\"%s\").", experimentCode));

      return experiments.getObjects().isEmpty() ? null : experiments.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getExperimentByCode(\"%s\") returned null.", experimentCode));
      return null;
    }
  }

  /**
   * Get experiment of the provided experiment identifier.
   * @param experimentIdentifier Code of the openBIS experiment
   * @return openBIS v3 Experiment
   */
  @Override
  public Experiment getExperimentById(String experimentId) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria esc = new ExperimentSearchCriteria();
      esc.withId().thatEquals( new ExperimentIdentifier(experimentId) );

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, esc, fetchExperimentsCompletely());

      if (experiments.getTotalCount() == 0)
        logger.info(String.format("No experiments found with getExperimentById(\"%s\").", experimentId));

      return experiments.getObjects().isEmpty() ? null : experiments.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getExperimentById(\"%s\") returned null.", experimentId));
      return null;
    }
  }

  /**
   * Get all experiments assigned to the provided experiment type.
   * @param typeCode Code of the openBIS experiment type
   * @return List of openBIS v3 Experiment
   */
  @Override
  public List<Experiment> getExperimentsOfType(String typeCode) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria esc = new ExperimentSearchCriteria();
      esc.withType().withCode().thatEquals(typeCode);

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, esc, fetchExperimentsCompletely());

      if (experiments.getTotalCount() == 0)
        logger.info(String.format("No experiments found with getExperimentsOfType(\"%s\").", typeCode));

      return experiments.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getExperimentsOfType(\"%s\") returned null.", typeCode));
      return null;
    }
  }

  /**
   * Get experiment type for the provided openBIS experiment type code.
   * @param typeCode Code of the openBIS experiment type
   * @return openBIS v3 ExperimentType
   */
  @Override
  public ExperimentType getExperimentTypeByString(String typeCode) {
    ensureLoggedIn();

    try {
      ExperimentTypeSearchCriteria etsc = new ExperimentTypeSearchCriteria();
      etsc.withCode().thatContains(typeCode);

      SearchResult<ExperimentType> experimentTypes = v3.searchExperimentTypes(sessionToken, etsc, fetchExperimentTypesCompletely());

      if (experimentTypes.getTotalCount() == 0)
        logger.info(String.format("No experiment types found with getExperimentTypeByString(\"%s\").", typeCode));

      return experimentTypes.getObjects().isEmpty() ? null : experimentTypes.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiment types. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getExperimentTypeByString(\"%s\") returned null.", typeCode));
      return null;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Sample / SampleType ---------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Get all samples which are registered in the openBIS instance.
   * @return List of openBIS v3 Sample
   */
  public List<Sample> listSamples() {
    ensureLoggedIn();

    try {
      SearchResult<Sample> samples = v3.searchSamples(sessionToken, new SampleSearchCriteria(), fetchSamplesCompletely());

      if (samples.getTotalCount() == 0)
        logger.info("No samples found with listSamples().");

      return samples.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn("listSamples() returned null.");
      return null;
    }
  }

  /**
   * Get all Samples which are available to the provided user in the openBIS instance.
   * @param user openBIS user name
   * @return List of openBIS v3 Sample
   */
  public List<Sample> listSamplesForUser(String user) {
    ensureLoggedIn(user);

    try {
      SearchResult<Sample> samples = v3.searchSamples(sessionToken, new SampleSearchCriteria(), fetchSamplesCompletely());

      if (samples.getTotalCount() == 0)
        logger.info(String.format("No samples found with listSamplesForUser(\"%s\").", user));

      return samples.getObjects();

    } catch (UserFailureException ufe) {
      logger.error(String.format("Could not fetch samples of user %s. Does this user exist in openBIS?", user));
      logger.warn(String.format("listSamplesForUser(\"%s\") returned null.", user));
      return null;
    }
  }

  /**
   * Get all samples registered under the provided space code.
   * @param spaceCode Code of the openBIS space
   * @return List of openBIS v3 Sample
   */
  @Override
  public List<Sample> getSamplesOfSpace(String spaceCode) {
    ensureLoggedIn();

    try {
      SampleSearchCriteria ssc = new SampleSearchCriteria();
      ssc.withSpace().withCode().thatEquals(spaceCode);

      SearchResult<Sample> samples = v3.searchSamples(sessionToken, ssc, fetchSamplesCompletely());

      if (samples.getTotalCount() == 0)
        logger.info(String.format("No samples found with getSamplesOfSpace(\"%s\").", spaceCode));

      return samples.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getSamplesofSpace(\"%s\") returned null.", spaceCode));
      return null;
    }
  }

  /**
   * Get all samples registered under the provided project code or identifier.
   * @param projectCodeOrIdentifier Code or identifier of the openBIS project
   * @return List of openBIS v3 Sample
   */
  @Override
  public List<Sample> getSamplesOfProject(String projectCodeOrIdentifier) {
    ensureLoggedIn();

    try {
      SampleSearchCriteria ssc = new SampleSearchCriteria();
      ssc.withOrOperator();
      ssc.withExperiment().withProject().withCode().thatEquals(projectCodeOrIdentifier);
      ssc.withExperiment().withProject().withId().thatEquals(new ProjectIdentifier(projectCodeOrIdentifier));

      SearchResult<Sample> samples = v3.searchSamples(sessionToken, ssc, fetchSamplesCompletely());

      if (samples.getTotalCount() == 0)
        logger.info(String.format("No samples found with getSamplesOfProject(\"%s\").", projectCodeOrIdentifier));

      return samples.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getSamplesOfProject(\"%s\") returned null.", projectCodeOrIdentifier));
      return null;
    }
  }

  /**
   * Get all samples registered under the provided experiment code or identifier.
   * @param experimentCodeOrIdentifier Code or identifier of the openBIS experiment
   * @return List of openBIS v3 Sample
   */
  @Override
  public List<Sample> getSamplesOfExperiment(String experimentCodeOrIdentifier) {
    ensureLoggedIn();

    try {
      SampleSearchCriteria ssc = new SampleSearchCriteria();

      if (experimentCodeOrIdentifier.startsWith("/"))
        ssc.withExperiment().withIdentifier().thatEquals(experimentCodeOrIdentifier);
      else
        ssc.withExperiment().withCode().thatEquals(experimentCodeOrIdentifier);

      SearchResult<Sample> samplesOfExperiment = v3.searchSamples(sessionToken, ssc, fetchSamplesCompletely());

      if (samplesOfExperiment.getTotalCount() == 0)
        logger.info(String.format("No samples found with getSamplesofExperiment(\"%s\").", experimentCodeOrIdentifier));

      return samplesOfExperiment.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getSamplesofExperiment(\"%s\") returned null.", experimentCodeOrIdentifier));
      return null;
    }
  }

  /**
   * Get sample of the provided sample code or identifier.
   * @param sampleCodeOrIdentifier Code or identifier of the openBIS sample
   * @return openBIS v3 Sample
   */
  public Sample getSample(String sampleCodeOrIdentifier) {
    if (sampleCodeOrIdentifier.startsWith("/"))
      return getSampleByIdentifier(sampleCodeOrIdentifier);  // ensureLoggedIn() is called inside
    else
      return getSampleByCode(sampleCodeOrIdentifier);  // ensureLoggedIn() is called inside
  }

  /**
   * Get sample of the provided sample code.
   * @param sampleCode Code of the openBIS sample
   * @return openBIS v3 Sample
   */
  @Override
  public Sample getSampleByCode(String sampleCode) {
    ensureLoggedIn();

    try {
      SampleSearchCriteria ssc = new SampleSearchCriteria();
      ssc.withCode().thatEquals(sampleCode);

      SearchResult<Sample> samples = v3.searchSamples(sessionToken, ssc, fetchSamplesCompletely());

      if (samples.getTotalCount() == 0)
        logger.info(String.format("No samples found with getSampleByCode(\"%s\").", sampleCode));

      return samples.getObjects().isEmpty() ? null : samples.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getSampleByCode(\"%s\") returned null.", sampleCode));
      return null;
    }
  }

  /**
   * Get sample of the provided sample identifier.
   * @param sampleIdentifier Identifier of the openBIS sample
   * @return openBIS v3 Sample; null if search is empty
   */
  @Override
  public Sample getSampleByIdentifier(String sampleIdentifier) {
    ensureLoggedIn();

    try {
      SampleSearchCriteria ssc = new SampleSearchCriteria();
      ssc.withIdentifier().thatEquals(sampleIdentifier);

      SearchResult<Sample> samples = v3.searchSamples(sessionToken, ssc, fetchSamplesCompletely());

      if (samples.getTotalCount() == 0)
        logger.info(String.format("No samples found with getSampleByIdentifier(\"%s\").", sampleIdentifier));

      return samples.getObjects().isEmpty() ? null : samples.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getSampleByIdentifier(\"%s\") returned null.", sampleIdentifier));
      return null;
    }
  }

  /**
   * Get samples with its parents and children objects also fetched entirely.
   * @param sampleCode Code of the openBIS sample
   * @return List of openBIS v3 Sample
   */
  @Override
  public List<Sample> getSamplesWithParentsAndChildren(String sampleCode) {
    // ToDo: Unclear if parents and children should be fetched or directly included into the list.
    ensureLoggedIn();

    try {
      SampleSearchCriteria ssc = new SampleSearchCriteria();
      ssc.withCode().thatEquals(sampleCode);

      SampleFetchOptions sampleFetchOptions = fetchSamplesCompletely();
      sampleFetchOptions.withChildrenUsing(fetchSamplesCompletely());
      sampleFetchOptions.withParentsUsing(fetchSamplesCompletely());

      SearchResult<Sample> samples = v3.searchSamples(sessionToken, ssc, fetchSamplesCompletely());

      if (samples.getTotalCount() == 0)
        logger.info(String.format("No samples found with getSamplesWithParentsAndChildren(\"%s\").", sampleCode));

      return samples.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getSamplesWithParentsAndChildren(\"%s\") returned null.", sampleCode));
      return null;
    }
  }

  /**
   * Get all samples of the provided sample type.
   * @param typeCode Code of the openBIS sample type
   * @return List of openBIS v3 Sample
   */
  @Override
  public List<Sample> getSamplesOfType(String typeCode) {
    ensureLoggedIn();

    try {
      SampleSearchCriteria ssc = new SampleSearchCriteria();
      ssc.withType().withCode().thatEquals(typeCode);

      SearchResult<Sample> samples = v3.searchSamples(sessionToken, ssc, fetchSamplesCompletely());

      if (samples.getTotalCount() == 0)
        logger.info(String.format("No samples found with getSamplesOfType(\"%s\").", typeCode));

      return samples.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getSamplesOfType(\"%s\") returned null.", typeCode));
      return null;
    }
  }


  /**
   * Get sample type for the provided openBIS sample type code.
   * @param typeCode Code of the openBIS sample type
   * @return openBIS v3 SampleType
   */
  @Override
  public SampleType getSampleTypeByString(String typeCode) {
    ensureLoggedIn();

    try {
      SampleTypeSearchCriteria stsc = new SampleTypeSearchCriteria();
      stsc.withCode().thatEquals(typeCode);

      SearchResult<SampleType> sampleTypes = v3.searchSampleTypes(sessionToken, stsc, fetchSampleTypesCompletely());

      if (sampleTypes.getTotalCount() == 0)
        logger.info(String.format("No sample types found with getSampleTypeByString(\"%s\").", typeCode));

      return sampleTypes.getObjects().isEmpty() ? null : sampleTypes.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples types. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getSampleTypeByString(\"%s\") returned null.", typeCode));
      return null;
    }
  }

  /**
   * Get map with all sample type codes as key and their SampleTypes as value.
   * @return Map with sample type code as key and openBIS v3 SampleType as value
   */
  @Override
  public Map<String, SampleType> getSampleTypes() {
    ensureLoggedIn();

    try {
      SearchResult<SampleType> sampleTypes = v3.searchSampleTypes(sessionToken, new SampleTypeSearchCriteria(), fetchSampleTypesCompletely());

      if (sampleTypes.getTotalCount() == 0)
        logger.info("No sample types found with getSampleTypes().");

      Map<String, SampleType> codeToSampleType = new HashMap<>();
      for (SampleType t : sampleTypes.getObjects()) {
        codeToSampleType.put(t.getCode(), t);
      }

      return codeToSampleType;

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples types. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn("getSampleTypes() returned null.");
      return null;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- DataSets --------------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Get all datasets which are registered in the openBIS instance.
   * @return List of openBIS v3 DataSet
   */
  public List<DataSet> listDatasets() {
    ensureLoggedIn();

    try {
      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, new DataSetSearchCriteria(), fetchDataSetsCompletely());

      if (datasets.getTotalCount() == 0)
        logger.info("No datasets found with listDatasets().");

      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn("listDatasets() returned null.");
      return null;
    }
  }

  /**
   * Get all datasets which are available to the provided user in the openBIS instance.
   * @param user openBIS user name
   * @return List of openBIS v3 DataSet
   */
  public List<DataSet> listDatasetsForUser(String user) {
    ensureLoggedIn(user);

    try {
      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, new DataSetSearchCriteria(), fetchDataSetsCompletely());

      if (datasets.getTotalCount() == 0)
        logger.info(String.format("No datasets found with listDatasetsForUser(\"%s\").", user));

      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error(String.format("Could not fetch datasets of user %s. Does this user exist in openBIS?", user));
      logger.warn(String.format("listDatasetsForUser(\"%s\") returned null.", user));
      return null;
    }
  }

  /**
   * Get all datasets registered under the provided openBIS space code.
   * @param spaceCode Code of the openBIS space
   * @return List of openBIS v3 DataSet
   */
  @Override
  public List<DataSet> getDataSetsOfSpace(String spaceCode) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria dssc = new DataSetSearchCriteria();
      dssc.withSample().withSpace().withCode().thatEquals(spaceCode);

      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, dssc, fetchDataSetsCompletely());

      if (datasets.getTotalCount() == 0)
        logger.info(String.format("No datasets found with getDataSetsOfSpace(\"%s\").", spaceCode));

      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getDataSetsOfSpace(\"%s\") returned null.", spaceCode));
      return null;
    }
  }

  /**
   * Get all datasets registered under the provided list of openBIS projects.
   * @param projects List of openBIS v3 Project
   * @return List of openBIS v3 DataSet
   */
  @Override
  public List<DataSet> getDataSetsOfProjects(List<Project> projects) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria dssc = new DataSetSearchCriteria();
      dssc.withSample().withProject().withCodes().thatIn( projects.stream().map(Project::getCode).collect(Collectors.toList()) );

      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, new DataSetSearchCriteria(), fetchDataSetsCompletely());

      if (datasets.getTotalCount() == 0)
        logger.info(String.format("No datasets found with getDataSetsOfProjects(\"%s\").", projects));

      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getDataSetsOfProjects(\"%s\") returned null.", projects));
      return null;
    }
  }

  /**
   * Get all datasets registered under the provided project code or identifier.
   * @param projectCodeIdentifier Code or identifier of the openBIS project
   * @return List of openBIS v3 DataSet
   */
  @Override
  public List<DataSet> getDataSetsOfProject(String projectCodeIdentifier) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria dssc = new DataSetSearchCriteria();
      dssc.withOrOperator();
      dssc.withSample().withProject().withCode().thatEquals(projectCodeIdentifier);
      dssc.withSample().withProject().withId().thatEquals( new ProjectIdentifier(projectCodeIdentifier) );

      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, new DataSetSearchCriteria(), fetchDataSetsCompletely());

      if (datasets.getTotalCount() == 0)
        logger.info(String.format("No datasets found with getDataSetsOfProject(\"%s\").", projectCodeIdentifier));

      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getDataSetsOfProjects(\"%s\") returned null.", projectCodeIdentifier));
      return null;
    }
  }

  /**
   * Get all datasets registered under the provided experiment codes.
   * @param experimentCodes List of experiment codes registered in openBIS
   * @return List of openBIS v3 DataSet
   */
  @Override
  public List<DataSet> listDataSetsForExperiments(List<String> experimentCodes) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria dssc = new DataSetSearchCriteria();
      dssc.withExperiment().withCodes().thatIn( experimentCodes );

      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, new DataSetSearchCriteria(), fetchDataSetsCompletely());

      if (datasets.getTotalCount() == 0)
        logger.info(String.format("No datasets found with listDataSetsForExperiments(\"%s\").", experimentCodes));

      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("listDataSetsForExperiments(\"%s\") returned null.", experimentCodes));
      return null;
    }
  }

  /**
   * Get all datasets registered under the provided experiment identifier.
   * @param experimentIdentifier Identifier of the openBIS experiment
   * @return List of openBIS v3 DataSet
   */
  @Override
  public List<DataSet> getDataSetsOfExperimentByIdentifier(String experimentIdentifier) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria dssc = new DataSetSearchCriteria();
      dssc.withExperiment().withIdentifier().thatEquals(experimentIdentifier);

      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, dssc, fetchDataSetsCompletely());

      if (datasets.getTotalCount() == 0)
        logger.info(String.format("No datasets found with getDataSetsOfExperimentByIdentifier(\"%s\").", experimentIdentifier));

      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getDataSetsOfExperimentByIdentifier(\"%s\") returned null.", experimentIdentifier));
      return null;
    }
  }

  /**
   * Get all datasets registered under the provided experiment PermID.
   * @param experimentPermID PermID of the openBIS experiment
   * @return List of openBIS v3 DataSet
   */
  @Override
  public List<DataSet> getDataSetsOfExperiment(String experimentPermID) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria dssc = new DataSetSearchCriteria();
      dssc.withExperiment().withPermId().thatEquals(experimentPermID);

      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, dssc, fetchDataSetsCompletely());

      if (datasets.getTotalCount() == 0)
        logger.info(String.format("No datasets found with getDataSetsOfExperiment(\"%s\").", experimentPermID));

      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getDataSetsOfExperiment(\"%s\") returned null.", experimentPermID));
      return null;
    }
  }

  /**
   * Get all datasets registered under the provided sample codes.
   * @param sampleCodes List of sample codes registered in openBIS
   * @return List of openBIS v3 DataSet
   */
  @Override
  public List<DataSet> listDataSetsForSamples(List<String> sampleCodes) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria dssc = new DataSetSearchCriteria();
      dssc.withSample().withCodes().thatIn( sampleCodes );

      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, new DataSetSearchCriteria(), fetchDataSetsCompletely());

      if (datasets.getTotalCount() == 0)
        logger.info(String.format("No datasets found with listDataSetsForSamples(\"%s\").", sampleCodes));

      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("listDataSetsForSamples(\"%s\") returned null.", sampleCodes));
      return null;
    }
  }

  /**
   * Get all datasets registered under the provided sample code or identifier.
   * @param sampleCodeOrIdentifier Code or Identifier of the openBIS sample
   * @return List of openBIS v3 DataSet
   */
  @Override
  public List<DataSet> getDataSetsOfSample(String sampleCodeOrIdentifier) {
    if (sampleCodeOrIdentifier.startsWith("/"))
      return getDataSetsOfSampleByIdentifier(sampleCodeOrIdentifier);  // ensureLoggedIn() is called inside
    else
      return getDataSetsOfSampleByCode(sampleCodeOrIdentifier);  // ensureLoggedIn() is called inside
  }

  /**
   * Get all datasets registered under the provided sample code.
   * @param sampleCode Code of the openBIS sample
   * @return List of openBIS v3 DataSet
   */
  @Override
  public List<DataSet> getDataSetsOfSampleByCode(String sampleCode) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria dssc = new DataSetSearchCriteria();
      dssc.withSample().withCode().thatEquals(sampleCode);

      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, dssc, fetchDataSetsCompletely());

      if (datasets.getTotalCount() == 0)
        logger.info(String.format("No datasets found with getDataSetsOfSampleByCode(\"%s\").", sampleCode));

      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getDataSetsOfSampleByCode(\"%s\") returned null.", sampleCode));
      return null;
    }
  }

  /**
   * Get all datasets registered under the provided sample identifier.
   * @param sampleIdentifier Identifier of the openBIS sample
   * @return List of openBIS v3 DataSet
   */
  @Override
  public List<DataSet> getDataSetsOfSampleByIdentifier(String sampleIdentifier) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria ssc = new DataSetSearchCriteria();
      ssc.withSample().withIdentifier().thatEquals(sampleIdentifier);

      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, ssc, fetchDataSetsCompletely());

      if (datasets.getTotalCount() == 0)
        logger.info(String.format("No datasets found with getDataSetsOfSampleByIdentifier(\"%s\").", sampleIdentifier));

      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getDataSetsOfSampleByIdentifier(\"%s\") returned null.", sampleIdentifier));
      return null;
    }
  }

  /**
   * Get all datasets of the provided dataset type.
   * @param typeCode Code of the openBIS dataset type
   * @return List of openBIS v3 DataSet
   */
  @Override
  public List<DataSet> getDataSetsByType(String typeCode) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria sc = new DataSetSearchCriteria();
      sc.withType().withCode().thatEquals(typeCode);

      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, sc, fetchDataSetsCompletely());

      if (datasets.getTotalCount() == 0)
        logger.info(String.format("No datasets found with getDataSetsByType(\"%s\").", typeCode));

      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getDataSetsByType(\"%s\") returned null.", typeCode));
      return null;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Vocabulary / VocabularyTerm -------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Get all vocabularies which are registered in the openBIS instance.
   * @return List of openBIS v3 Vocabulary
   */
  public List<Vocabulary> listVocabulary() {
    ensureLoggedIn();

    try {
      SearchResult<Vocabulary> vocabularies = v3.searchVocabularies(sessionToken, new VocabularySearchCriteria(), fetchVocabularyCompletely());

      if (vocabularies.getTotalCount() == 0)
        logger.info("No vocabularies found with listVocabulary().");

      return vocabularies.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch vocabularies. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn("listVocabulary() returned null.");
      return null;
    }
  }

  /**
   * Get all vocabualaries which are available to the provided user in the openBIS instance.
   * @param user openBIS user name
   * @return List of openBIS v3 Vocabulary
   */
  public List<Vocabulary> listVocabularyForUser(String user) {
    ensureLoggedIn(user);

    try {
      SearchResult<Vocabulary> vocabularies = v3.searchVocabularies(sessionToken, new VocabularySearchCriteria(), fetchVocabularyCompletely());

      if (vocabularies.getTotalCount() == 0)
        logger.info(String.format("No vocabularies found with listVocabularyForUser(\"%s\").", user));

      return vocabularies.getObjects();

    } catch (UserFailureException ufe) {
      logger.error(String.format("Could not fetch vocabularies of user %s. Does this user exist in openBIS?", user));
      logger.warn(String.format("listVocabularyForUser(\"%s\") returned null.", user));
      return null;
    }
  }

  /**
   * Get vocabulary of the provided vocabulary code.
   * @param vocabularyCode Code of the openBIS vocabulary
   * @return openBIS v3 Vocabulary
   */
  @Override
  public Vocabulary getVocabulary(String vocabularyCode) {
    ensureLoggedIn();

    try {
      VocabularySearchCriteria vsc = new VocabularySearchCriteria();
      vsc.withCode().thatEquals(vocabularyCode);

      SearchResult<Vocabulary> vocabularies = v3.searchVocabularies(sessionToken, vsc, fetchVocabularyCompletely());

      if (vocabularies.getTotalCount() == 0)
        logger.info(String.format("No vocabularies found with getVocabulary(\"%s\").", vocabularyCode));

      return vocabularies.getObjects().isEmpty() ? null : vocabularies.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch vocabularies. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getVocabulary(\"%s\") returned null.", vocabularyCode));
      return null;
    }
  }

  /**
   * Get all vocabulary terms of the vocabulary assigned to the provided property type.
   * @param property openBIS v3 PropertyType
   * @return List of vocabulary term codes
   */
  @Override
  public List<String> listVocabularyTermsForProperty(PropertyType property) {
    ensureLoggedIn();

    try {
      PropertyTypeSearchCriteria ptsc = new PropertyTypeSearchCriteria();
      ptsc.withCode().thatEquals(property.getCode());

      List<String> vocabTerms = new ArrayList<>();
      SearchResult<PropertyType> properties = v3.searchPropertyTypes(sessionToken, ptsc, fetchPropertyTypeWithVocabularyAndTerms());

      if (properties.getObjects().isEmpty()) {
        logger.warn(String.format("Seems like property type %s does not exist anymore. Returning empty list.", property.getCode()));
        return vocabTerms;
      }

      try {
        PropertyType pt = properties.getObjects().get(0);
        vocabTerms.addAll( pt.getVocabulary().getTerms().stream().map(VocabularyTerm::getCode).collect(Collectors.toList()) );
        return vocabTerms;

      } catch (NotFetchedException nfe) {
        logger.warn("PropertyType %s has no fetched vocabulary and/or vocabulary terms. Returning empty list.");
        return vocabTerms;
      }

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch property types. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("listVocabularyTermsForProperty(\"%s\") returned null.", property.toString()));
      return null;
    }
  }

  /**
   * Get a map of vocabulary term codes as keys and labels as values
   * from a vocabulary of the provided vocabulary code.
   * @param vocabularyCode Code of the openBIS vocabulary
   * @return Map with vocabulary term codes as keys and labels as values
   */
  @Override
  public Map<String, String> getVocabCodesAndLabelsForVocab(String vocabularyCode) {
    ensureLoggedIn();

    try {
      Map<String, String> codesAndLabels = new HashMap<>();
      VocabularySearchCriteria vsc = new VocabularySearchCriteria();
      vsc.withCode().thatEquals(vocabularyCode);

      SearchResult<Vocabulary> vocabularies = v3.searchVocabularies(sessionToken,vsc, fetchVocabularyCompletely());

      if (vocabularies.getObjects().isEmpty()) {
        logger.info(String.format("No vocabularies found with getVocabCodesAndLabelsForVocab(\"%s\").", vocabularyCode));
        return codesAndLabels;
      }

      Vocabulary vocabulary = vocabularies.getObjects().get(0);
      try {
          for (VocabularyTerm vt : vocabulary.getTerms()) {
            // codesAndLabels.put( vt.getLabel(), vt.getCode() );  // Note: Why first label and then code?
            codesAndLabels.put( vt.getCode(), vt.getLabel() );
          }
          return codesAndLabels;

      } catch (NotFetchedException nfe) {
        logger.warn(String.format("Vocabulary %s has no fetched vocabulary terms. Returning empty map.", vocabulary.getCode()));
        return codesAndLabels;
      }

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch vocabulary. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getVocabCodesAndLabelsForVocab(\"%s\") returned null.", vocabularyCode));
      return null;
    }
  }

  /**
   * Get all vocabulary term codes from a vocabulary of the provided vocabulary code.
   * @param vocabularyCode Code of the openBIS vocabulary
   * @return List of vocabulary term codes
   */
  @Override
  public List<String> getVocabCodesForVocab(String vocabularyCode) {
    ensureLoggedIn();

    try {
      List<String> vocabCodes = new ArrayList<>();
      VocabularySearchCriteria vsc = new VocabularySearchCriteria();
      vsc.withCode().thatEquals(vocabularyCode);

      SearchResult<Vocabulary> vocabularies = v3.searchVocabularies(sessionToken, vsc, fetchVocabularyCompletely());

      if (vocabularies.getObjects().isEmpty()) {
        logger.info(String.format("No vocabularies found with getVocabCodesForVocab(\"%s\").", vocabularyCode));
        return vocabCodes;
      }

      Vocabulary vocabulary = vocabularies.getObjects().get(0);
      try {
        vocabCodes.addAll( vocabulary.getTerms().stream().map(VocabularyTerm::getCode).collect(Collectors.toList()) );
        return vocabCodes;

      } catch (NotFetchedException nfe) {
        logger.warn(String.format("Vocabulary %s has no fetched vocabulary terms. Returning empty map.", vocabulary.getCode()));
        return vocabCodes;
      }

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch vocabularies. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getVocabCodesForVocab(\"%s\") returned null.", vocabularyCode));
      return null;
    }
  }

  /**
   * Get the label of a vocabulary term for the provided vocabulary term code.
   * @param propertyType openBIS v3 PropertyType (not used...)
   * @param vocabularyTermCode Code of openBIS VocabularyTerm
   * @return Label of vocabulary term
   */
  @Override
  public String getCVLabelForProperty(PropertyType propertyType, String vocabularyTermCode) {
    ensureLoggedIn();

    try {
      VocabularyTermSearchCriteria vtsc = new VocabularyTermSearchCriteria();
      vtsc.withCode().thatEquals(vocabularyTermCode);

      SearchResult<VocabularyTerm> vocabularyTerms = v3.searchVocabularyTerms(sessionToken, vtsc, new VocabularyTermFetchOptions());

      if (vocabularyTerms.getTotalCount() == 0)
        logger.info(String.format("No vocabulary terms found with getCVLabelForProperty(\"%s\").", vocabularyTermCode));

      return vocabularyTerms.getObjects().isEmpty() ? null : vocabularyTerms.getObjects().get(0).getLabel();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch vocabulary terms. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getCVLabelForProperty(%s, \"%s\") returned null.", propertyType, vocabularyTermCode));
      return null;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Attachments ------------------------------------------------------------------ */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Get all attachments registered under the provided sample identifier.
   * @param sampleIdentifier Identifier of the openBIS sample
   * @return List of openBIS v3 Attachment
   */
  @Override
  public List<Attachment> listAttachmentsForSampleByIdentifier(String sampleIdentifier) {
    ensureLoggedIn();

    return getSample(sampleIdentifier).getAttachments();
  }

  /**
   * Get all attachments registered under the provided project identifier.
   * @param projectIdentifier Identifier of the openBIS project
   * @return List of openBIS v3 Attachment
   */
  @Override
  public List<Attachment> listAttachmentsForProjectByIdentifier(String projectIdentifier) {
    ensureLoggedIn();

    return getProject(projectIdentifier).getAttachments();
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Exists Methods --------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Checks if space with provided space code exists in the openBIS instance.
   * @param spaceCode Code of an openBIS space
   * @return True if exists, else false
   */
  @Override
  public boolean spaceExists(String spaceCode) {
    return listSpaces().contains(spaceCode);  // ensureLoggedIn is called in listSpaces()
  }

  /**
   * Checks if the project with the provided project code exists in the openBIS instance
   * under the condition that the project is registered under the provided space code.
   * @param spaceCode Code of an openBIS space
   * @param projectCode Code of an openBIS project
   * @return True if exists, else false
   */
  @Override
  public boolean projectExists(String spaceCode, String projectCode) {
    ensureLoggedIn();

    try {
      ProjectSearchCriteria sc = new ProjectSearchCriteria();
      sc.withSpace().withCode().thatEquals(spaceCode);
      sc.withCode().thatEquals(projectCode);

      SearchResult<Project> projects = v3.searchProjects(sessionToken, sc, new ProjectFetchOptions());

      return spaceCode != null && projectCode != null && projects.getTotalCount() > 0;

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch projects. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("projectExists(\"%s\", \"%s\") returned false.", spaceCode, projectCode));
      return false;
    }
  }

  /**
   * Checks if the project with the provided project code or identifier exists in the openBIS instance.
   * @param projectCodeOrIdentifier Code or Identifier of an openBIS project
   * @return True if exists, else false
   */
  public boolean projectExists(String projectCodeOrIdentifier) {
    Project project = getProject(projectCodeOrIdentifier);  // ensureLoggedIn is called in getProject(String)

    return projectCodeOrIdentifier != null && project != null;
  }

  /**
   * Checks if the experiment with the provided experiment code exists in the openBIS instance
   * under the conditions that the project is registered under the provided space code and
   * the provided project code.
   * @param spaceCode Code of an openBIS space
   * @param projectCode Code of an openBIS project
   * @param experimentCode Code of an openBIS experiment
   * @return True if exists, else false
   */
  @Override
  public boolean expExists(String spaceCode, String projectCode, String experimentCode) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria sc = new ExperimentSearchCriteria();
      sc.withProject().withSpace().withCode().thatEquals(spaceCode);
      sc.withProject().withCode().thatEquals(projectCode);
      sc.withCode().thatEquals(experimentCode);

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, sc, new ExperimentFetchOptions());

      return spaceCode != null && projectCode != null && experimentCode != null && experiments.getTotalCount() != 0;

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("expExists(\"%s\", \"%s\", \"%s\") returned false.", spaceCode, projectCode, experimentCode));
      return false;
    }
  }

  /**
   * Checks if the experiment with the provided experiment code or Identifier exists in the openBIS instance.
   * @param experimentCodeOrIdentifier Code or Identifier of an openBIS experiment
   * @return True if exists, else false
   */
  public boolean experimentExists(String experimentCodeOrIdentifier) {
    Experiment experiment = getExperiment(experimentCodeOrIdentifier);   // ensureLoggedIn() is called in getExperiment(String)

    return experimentCodeOrIdentifier != null && experiment != null;
  }

  /**
   * Checks if the sample with the provided sample code or Identifier exists in the openBIS instance.
   * @param sampleCodeOrIdentifier Code or Identifier of an openBIS sample
   * @return True if exists, else false
   */
  @Override
  public boolean sampleExists(String sampleCodeOrIdentifier) {
    Sample sample = getSample(sampleCodeOrIdentifier);  // ensureLoggedIn() is called in getSample(String)

    return sampleCodeOrIdentifier != null && sample != null;
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Space creation --------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Get a {@link SpaceCreation} object that can be used to register a new space in openBIS
   * with {@link #createSpaces(List) createSpaces}.
   * @param code Unique space code which does not already exist in the openBIS instance
   * @return {@link SpaceCreation} with set code
   */
  public SpaceCreation prepareSpaceCreation(String code) {
    SpaceCreation space = new SpaceCreation();
    space.setCode(code);

    return space;
  }

  /**
   * Get a {@link SpaceCreation} object that can be used to register a new space in openBIS
   * with {@link #createSpaces(List) createSpaces}.
   * @param code Unique space code which does not already exist in the openBIS instance
   * @param description Description of the space
   * @return {@link SpaceCreation} with set code and description
   */
  public SpaceCreation prepareSpaceCreation(String code, String description) {
    SpaceCreation space = new SpaceCreation();
    space.setCode(code);
    space.setDescription(description);

    return space;
  }

  /**
   * Create a new space in the openBIS instance with the provided code.
   * @param code Unique space code which does not already exist in the openBIS instance
   * @return {@link SpacePermId} of the newly registered space; null if registration fails
   */
  public SpacePermId createSpace(String code) {
    ensureLoggedIn();

    try {
      SpaceCreation space = prepareSpaceCreation(code);

      return v3.createSpaces(sessionToken, Collections.singletonList(space)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create space. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createSpace(\"%s\") returned null.", code));
      return null;
    }
  }

  /**
   * Create a new space in the openBIS instance with the provided code and description.
   * @param code Unique space code which does not already exist in the openBIS instance
   * @param description Description of the space
   * @return {@link SpacePermId} of the newly registered space; null if registration fails
   */
  public SpacePermId createSpace(String code, String description) {
    ensureLoggedIn();

    try {
      SpaceCreation space = prepareSpaceCreation(code, description);

      return v3.createSpaces(sessionToken, Collections.singletonList(space)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create space. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createSpace(\"%s\", \"%s\") returned null.", code, description));

      return null;
    }
  }

  /**
   * Create multiple new spaces in the openBIS instance.
   * @param spaces List of {@link SpaceCreation}, each at least with a unique space code
   *               which does not already exist in the openBIS instance
   * @return List of {@link SpacePermId} of the newly registered spaces; null if registration fails
   */
  public List<SpacePermId> createSpaces(List<SpaceCreation> spaces) {
    ensureLoggedIn();

    try {
      return v3.createSpaces(sessionToken, spaces);

    } catch (UserFailureException ufe) {
      logger.error("Could not create spaces. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn("createSpaces(List<ProjectCreation>) returned null.");
      return null;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Project creation ------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Get a {@link ProjectCreation} object that can be used to register a new project in openBIS
   * with {@link #createProjects(List) createProjects}.
   * @param code Unique project code within space of creation
   * @param space Code of space that exists in the openBIS instance
   * @return {@link ProjectCreation} with set code and space
   */
  public ProjectCreation prepareProjectCreation(String code, String space) {
    ProjectCreation project = new ProjectCreation();
    project.setCode(code);
    project.setSpaceId( new SpacePermId(space) );

    return project;
  }

  /**
   * Get a {@link ProjectCreation} object that can be used to register a new project in openBIS
   * with {@link #createProjects(List) createProjects}.
   * @param code Unique project code within space of creation
   * @param space Code of space that exists in the openBIS instance
   * @param description Description of the project
   * @return {@link ProjectCreation} with set code, space and description
   */
  public ProjectCreation prepareProjectCreation(String code, String space, String description) {
    ProjectCreation project = new ProjectCreation();
    project.setCode(code);
    project.setDescription(description);
    project.setSpaceId( new SpacePermId(space) );

    return project;
  }

  /**
   * Create a new project in the openBIS instance under the provided space code.
   * @param code Unique project code within space of creation
   * @param space Code of space that exists in the openBIS instance
   * @return {@link SpacePermId} of the newly registered project; null if registration fails
   */
  public ProjectPermId createProject(String code, String space) {
    ensureLoggedIn();

    try {
      ProjectCreation project = prepareProjectCreation(code, space);

      return v3.createProjects(sessionToken, Collections.singletonList(project)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create project. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createProject(\"%s\", \"%s\") returned null.", code, space));
      return null;
    }
  }

  /**
   * Create a new project in the openBIS instance under the provided space code
   * with the provided project code and description.
   * @param code Unique project code within space of creation
   * @param space Code of space that exists in the openBIS instance
   * @param description Description of the project
   * @return {@link SpacePermId} of the newly registered project; null if registration fails
   */
  public ProjectPermId createProject(String code, String space, String description) {
    ensureLoggedIn();

    try {
      ProjectCreation project = prepareProjectCreation(code, space, description);

      return v3.createProjects(sessionToken, Collections.singletonList(project)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create project. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createProject(\"%s\", \"%s\", \"%s\") returned null.", code, space, description));
      return null;
    }
  }

  /**
   * Create multiple new projects in the openBIS instance.
   * @param projects List of {@link ProjectCreation}
   * @return List of {@link ProjectPermId} of the newly registered projects; null if registration fails
   */
  public List<ProjectPermId> createProjects(List<ProjectCreation> projects) {
    ensureLoggedIn();

    try {
      return v3.createProjects(sessionToken, projects);

    } catch (UserFailureException ufe) {
      logger.error("Could not create projects. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn("createProjects(List<ProjectCreation>) returned null.");
      return null;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Experiment creation ---------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Get a {@link ExperimentCreation} object that can be used to register a new experiment in openBIS
   * with {@link #createExperiments(List) createExperiments}.
   * @param code Unique experiment code within space of creation
   * @param type Experiment type that exists in the openBIS instance
   * @param projectIdentifier Identifier of project that exists in the openBIS instance
   * @return {@link ExperimentCreation} with set code, type and project identifier
   */
  public ExperimentCreation prepareExperimentCreation(String code, String type, String projectIdentifier) {
    ExperimentCreation experiment = new ExperimentCreation();
    experiment.setCode(code);
    experiment.setTypeId( new EntityTypePermId(type) );
    experiment.setProjectId( new ProjectIdentifier(projectIdentifier) );

    return experiment;
  }

  /**
   * Get a {@link ExperimentCreation} object that can be used to register a new experiment in openBIS
   * with {@link #createExperiments(List) createExperiments}.
   * @param code Unique experiment code within space of creation
   * @param type Experiment type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @return {@link ExperimentCreation} with set code, type, space and project
   */
  public ExperimentCreation prepareExperimentCreation(String code, String type, String space, String project) {
    ExperimentCreation experiment = new ExperimentCreation();
    experiment.setCode(code);
    experiment.setTypeId( new EntityTypePermId(type) );
    experiment.setProjectId( new ProjectIdentifier(space, project) );

    return experiment;
  }

  /**
   * Get a {@link ExperimentCreation} object that can be used to register a new experiment in openBIS
   * with {@link #createExperiments(List) createExperiments}.
   * @param code Unique experiment code within space of creation
   * @param type Experiment type that exists in the openBIS instance
   * @param projectIdentifier Identifier of project that exists in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link ExperimentCreation} with set code, type, project identifier and properties
   */
  public ExperimentCreation prepareExperimentCreation(String code, String type, String projectIdentifier,
                                                      Map<String, String> properties) {
    ExperimentCreation experiment = new ExperimentCreation();
    experiment.setCode(code);
    experiment.setTypeId( new EntityTypePermId(type) );
    experiment.setProjectId( new ProjectIdentifier(projectIdentifier) );
    experiment.setProperties(properties);

    return experiment;
  }

  /**
   * Get a {@link ExperimentCreation} object that can be used to register a new experiment in openBIS
   * with {@link #createExperiments(List) createExperiments}.
   * @param code Unique experiment code within space of creation
   * @param type Experiment type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link ExperimentCreation} with set code, type, space, project and properties
   */
  public ExperimentCreation prepareExperimentCreation(String code, String type, String space, String project,
                                                      Map<String, String> properties) {
    ExperimentCreation experiment = new ExperimentCreation();
    experiment.setCode(code);
    experiment.setTypeId( new EntityTypePermId(type) );
    experiment.setProjectId( new ProjectIdentifier(space, project) );
    experiment.setProperties(properties);

    return experiment;
  }

  /**
   * Create a new experiment in the openBIS instance under the provided project identifier.
   * @param code Unique experiment code within space of creation
   * @param type Experiment type that exists in the openBIS instance
   * @param projectIdentifier Identifier of project that exists in the openBIS instance
   * @return {@link ExperimentPermId} of the newly registered experiment; null if registration fails
   */
  public ExperimentPermId createExperiment(String code, String type, String projectIdentifier) {
    ensureLoggedIn();

    try {
      ExperimentCreation experiment = prepareExperimentCreation(code, type, projectIdentifier);

      return v3.createExperiments(sessionToken, Collections.singletonList(experiment)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create experiment. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createExperiment(\"%s\", \"%s\", \"%s\") returned null.", code, type, projectIdentifier));
      return null;
    }
  }

  /**
   * Create a new experiment in the openBIS instance under the provided space and project code.
   * @param code Unique experiment code within space of creation
   * @param type Experiment type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @return {@link ExperimentPermId} of the newly registered experiment; null if registration fails
   */
  public ExperimentPermId createExperiment(String code, String type, String space, String project) {
    ensureLoggedIn();

    try {
      ExperimentCreation experiment = prepareExperimentCreation(code, type, space, project);

      return v3.createExperiments(sessionToken, Collections.singletonList(experiment)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create experiment. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createExperiment(\"%s\", \"%s\", \"%s\", \"%s\") returned null.", code, type, space, project));
      return null;
    }
  }

  /**
   * Create a new experiment in the openBIS instance under the provided project identifier.
   * @param code Unique experiment code within space of creation
   * @param type Experiment type that exists in the openBIS instance
   * @param projectIdentifier Identifier of project that exists in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link ExperimentPermId} of the newly registered experiment; null if registration fails
   */
  public ExperimentPermId createExperiment(String code, String type, String projectIdentifier,
                                           Map<String, String> properties) {
    ensureLoggedIn();

    try {
      ExperimentCreation experiment = prepareExperimentCreation(code, type, projectIdentifier, properties);

      return v3.createExperiments(sessionToken, Collections.singletonList(experiment)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create experiment. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createExperiment(\"%s\", \"%s\", \"%s\", , \"%s\") returned null.",
                                code, type, projectIdentifier, properties));
      return null;
    }
  }

  /**
   * Create a new experiment in the openBIS instance under the provided space and project code.
   * @param code Unique experiment code within space of creation
   * @param type Experiment type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link ExperimentPermId} of the newly registered experiment; null if registration fails
   */
  public ExperimentPermId createExperiment(String code, String type, String space, String project,
                                           Map<String, String> properties) {
    ensureLoggedIn();

    try {
      ExperimentCreation experiment = prepareExperimentCreation(code, type, space, project, properties);

      return v3.createExperiments(sessionToken, Collections.singletonList(experiment)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create experiment. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createExperiment(\"%s\", \"%s\", \"%s\", \"%s\", \"%s\") returned null.",
                                code, type, space, project, properties));
      return null;
    }
  }

  /**
   * Create multiple new experiments in the openBIS instance.
   * @param experiments List of {@link ExperimentCreation}
   * @return List of {@link ExperimentPermId} of the newly registered experiments; null if registration fails
   */
  public List<ExperimentPermId> createExperiments(List<ExperimentCreation> experiments) {
    ensureLoggedIn();

    try {
      return v3.createExperiments(sessionToken, experiments);

    } catch (UserFailureException ufe) {
      logger.error("Could not create experiments. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn("createExperiment(List<ExperimentCreation>) returned null.");
      return null;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Sample creation -------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Get a {@link SampleCreation} object that can be used to register a new sample in openBIS
   * with {@link #createSamples(List) createSamples}.
   * @param code Unique sample code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @return {@link SampleCreation} with set code, type and space
   */
  public SampleCreation prepareSampleCreation(String code, String type, String space) {
    SampleCreation sample = new SampleCreation();
    sample.setCode(code);
    sample.setTypeId( new EntityTypePermId(type) );
    sample.setSpaceId( new SpacePermId(space) );

    return sample;
  }

  /**
   * Get a {@link SampleCreation} object that can be used to register a new sample in openBIS
   * with {@link #createSamples(List) createSamples}.
   * @param code Unique sample code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @return {@link SampleCreation} with set code, type, space and project
   */
  public SampleCreation prepareSampleCreation(String code, String type, String space, String project) {
    SampleCreation sample = new SampleCreation();
    sample.setCode(code);
    sample.setTypeId( new EntityTypePermId(type) );
    sample.setSpaceId( new SpacePermId(space) );
    sample.setProjectId( new ProjectIdentifier(space, project) );

    return sample;
  }

  /**
   * Get a {@link SampleCreation} object that can be used to register a new sample in openBIS
   * with {@link #createSamples(List) createSamples}.
   * @param code Unique sample code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @param experiment Code of experiment that exists in the openBIS instance
   * @return {@link SampleCreation} with set code, type, space, project and experiment
   */
  public SampleCreation prepareSampleCreation(String code, String type, String space, String project, String experiment) {
    SampleCreation sample = new SampleCreation();
    sample.setCode(code);
    sample.setTypeId( new EntityTypePermId(type) );
    sample.setSpaceId( new SpacePermId(space) );
    sample.setProjectId( new ProjectIdentifier(space, project) );
    sample.setExperimentId( new ExperimentIdentifier(space, project, experiment) );

    return sample;
  }

  /**
   * Get a {@link SampleCreation} object that can be used to register a new sample in openBIS
   * with {@link #createSamples(List) createSamples}.
   * @param code Unique sample code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param experimentIdentifier Identifier of experiment that exists in the openBIS instance
   * @return {@link SampleCreation} with set code, type, space, project and experiment
   */
  public SampleCreation prepareSampleCreation(String code, String type, ExperimentIdentifier experimentIdentifier) {

    String[] idParts = experimentIdentifier.toString().split("/");
    String space      = idParts[1];  // Space code is second as first is empty
    String project    = idParts[2];

    SampleCreation sample = new SampleCreation();
    sample.setCode(code);
    sample.setTypeId( new EntityTypePermId(type) );
    sample.setSpaceId( new SpacePermId(space) );
    sample.setProjectId( new ProjectIdentifier(space, project) );
    sample.setExperimentId( experimentIdentifier );

    return sample;
  }

  /**
   * Get a {@link SampleCreation} object that can be used to register a new sample in openBIS
   * with {@link #createSamples(List) createSamples}.
   * @param code Unique sample code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link SampleCreation} with set code, type, space, and properties
   */
  public SampleCreation prepareSampleCreation(String code, String type, String space,
                                              Map<String, String> properties) {
    SampleCreation sample = new SampleCreation();
    sample.setCode(code);
    sample.setTypeId( new EntityTypePermId(type) );
    sample.setSpaceId( new SpacePermId(space) );
    sample.setProperties(properties);

    return sample;
  }

  /**
   * Get a {@link SampleCreation} object that can be used to register a new sample in openBIS
   * with {@link #createSamples(List) createSamples}.
   * @param code Unique sample code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link SampleCreation} with set code, type, space, project and properties
   */
  public SampleCreation prepareSampleCreation(String code, String type, String space, String project,
                                              Map<String, String> properties) {
    SampleCreation sample = new SampleCreation();
    sample.setCode(code);
    sample.setTypeId( new EntityTypePermId(type) );
    sample.setSpaceId( new SpacePermId(space) );
    sample.setProjectId( new ProjectIdentifier(space, project) );
    sample.setProperties(properties);

    return sample;
  }

  /**
   * Get a {@link SampleCreation} object that can be used to register a new sample in openBIS
   * with {@link #createSamples(List) createSamples}.
   * @param code Unique sample code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @param experiment Code of experiment that exists in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link SampleCreation} with set code, type, space, project, experiment and properties
   */
  public SampleCreation prepareSampleCreation(String code, String type, String space, String project, String experiment,
                                              Map<String, String> properties) {
    SampleCreation sample = new SampleCreation();
    sample.setCode(code);
    sample.setTypeId( new EntityTypePermId(type) );
    sample.setSpaceId( new SpacePermId(space) );
    sample.setProjectId( new ProjectIdentifier(space, project) );
    sample.setExperimentId( new ExperimentIdentifier(space, project, experiment) );
    sample.setProperties(properties);

    return sample;
  }

  /**
   * Get a {@link SampleCreation} object that can be used to register a new sample in openBIS
   * with {@link #createSamples(List) createSamples}.
   * @param code Unique sample code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param experimentIdentifier Identifier of experiment that exists in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link SampleCreation} with set code, type, space, project, experiment and properties
   */
  public SampleCreation prepareSampleCreation(String code, String type, ExperimentIdentifier experimentIdentifier,
                                              Map<String, String> properties) {
    String[] idParts = experimentIdentifier.toString().split("/");
    String space      = idParts[1];  // Space code is second as first is empty
    String project    = idParts[2];

    SampleCreation sample = new SampleCreation();
    sample.setCode(code);
    sample.setTypeId( new EntityTypePermId(type) );
    sample.setSpaceId( new SpacePermId(space) );
    sample.setProjectId( new ProjectIdentifier(space, project) );
    sample.setExperimentId( experimentIdentifier );
    sample.setProperties(properties);

    return sample;
  }

  /**
   * Get a {@link SampleCreation} object that can be used to register a new sample in openBIS
   * with {@link #createSamples(List) createSamples}.
   * @param code Unique sample code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @param experiment Code of experiment that exists in the openBIS instance
   * @param parents List of {@link SampleIdentifier} that exist in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link SampleCreation} with set code, type, space, project, experiment, parents and properties
   */
  public SampleCreation prepareSampleCreation(String code, String type, String space, String project, String experiment,
                                              List<SampleIdentifier> parents, Map<String, String> properties) {
    SampleCreation sample = new SampleCreation();
    sample.setCode(code);
    sample.setTypeId( new EntityTypePermId(type) );
    sample.setExperimentId( new ExperimentIdentifier(space, project, experiment) );
    sample.setParentIds(parents);
    sample.setProperties(properties);

    return sample;
  }

  /**
   * Create a new sample in the openBIS instance under the provided space code.
   * @param code Unique experiment code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @return {@link SamplePermId} of the newly registered sample; null if registration fails
   */
  public SamplePermId createSample(String code, String type, String space) {
    ensureLoggedIn();

    try {
      SampleCreation sample = prepareSampleCreation(code, type, space);

      return v3.createSamples(sessionToken, Collections.singletonList(sample)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create sample. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createSample(\"%s\", \"%s\", \"%s\") returned null.", code, type, space));
      return null;
    }
  }

  /**
   * Create a new sample in the openBIS instance under the provided space and project code.
   * @param code Unique experiment code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @return {@link SamplePermId} of the newly registered sample; null if registration fails
   */
  public SamplePermId createSample(String code, String type, String space, String project) {
    ensureLoggedIn();

    try {
      SampleCreation sample = prepareSampleCreation(code, type, space, project);

      return v3.createSamples(sessionToken, Collections.singletonList(sample)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create sample. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createSample(\"%s\", \"%s\", \"%s\", \"%s\") returned null.",
              code, type, space, project));
      return null;
    }
  }

  /**
   * Create a new sample in the openBIS instance under the provided space, project and experiment code.
   * @param code Unique experiment code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @param experiment Code of experiment that exists in the openBIS instance
   * @return {@link SamplePermId} of the newly registered sample; null if registration fails
   */
  public SamplePermId createSample(String code, String type, String space, String project, String experiment) {
    ensureLoggedIn();

    try {
      SampleCreation sample = prepareSampleCreation(code, type, space, project, experiment);

      return v3.createSamples(sessionToken, Collections.singletonList(sample)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create sample. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createSample(\"%s\", \"%s\", \"%s\", \"%s\", \"%s\") returned null.",
              code, type, space, project, experiment));
      return null;
    }
  }

  /**
   * Create a new sample in the openBIS instance under the provided experiment identifier.
   * @param code Unique experiment code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param experimentIdentifier Identifier of experiment that exists in the openBIS instance
   * @return {@link SamplePermId} of the newly registered sample; null if registration fails
   */
  public SamplePermId createSample(String code, String type, ExperimentIdentifier experimentIdentifier) {
    ensureLoggedIn();

    try {
      SampleCreation sample = prepareSampleCreation(code, type, experimentIdentifier);

      return v3.createSamples(sessionToken, Collections.singletonList(sample)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create sample. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createSample(\"%s\", \"%s\", \"%s\") returned null.", code, type, experimentIdentifier));
      return null;
    }
  }

  /**
   * Create a new sample in the openBIS instance under the provided space code.
   * @param code Unique experiment code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link SamplePermId} of the newly registered sample; null if registration fails
   */
  public SamplePermId createSample(String code, String type, String space,
                                   Map<String, String> properties) {
    ensureLoggedIn();

    try {
      SampleCreation sample = prepareSampleCreation(code, type, space, properties);

      return v3.createSamples(sessionToken, Collections.singletonList(sample)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create sample. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createSample(\"%s\", \"%s\", \"%s\", \"%s\") returned null.",
              code, type, space, properties));
      return null;
    }
  }

  /**
   * Create a new sample in the openBIS instance under the provided space and project code.
   * @param code Unique experiment code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link SamplePermId} of the newly registered sample; null if registration fails
   */
  public SamplePermId createSample(String code, String type, String space, String project,
                                   Map<String, String> properties) {
    ensureLoggedIn();

    try {
      SampleCreation sample = prepareSampleCreation(code, type, space, project, properties);

      return v3.createSamples(sessionToken, Collections.singletonList(sample)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create sample. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createSample(\"%s\", \"%s\", \"%s\", \"%s\", \"%s\") returned null.",
              code, type, space, project, properties));
      return null;
    }
  }

  /**
   * Create a new sample in the openBIS instance under the provided space, project and experiment code.
   * @param code Unique experiment code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @param experiment Code of experiment that exists in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link SamplePermId} of the newly registered sample; null if registration fails
   */
  public SamplePermId createSample(String code, String type, String space, String project, String experiment,
                                   Map<String, String> properties) {
    ensureLoggedIn();

    try {
      SampleCreation sample = prepareSampleCreation(code, type, space, project, experiment, properties);

      return v3.createSamples(sessionToken, Collections.singletonList(sample)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create sample. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createSample(\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\") returned null.",
              code, type, space, project, experiment, properties));
      return null;
    }
  }

  /**
   * Create a new sample in the openBIS instance under the provided experiment identifier.
   * @param code Unique experiment code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param experimentIdentifier Identifier of experiment that exists in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link SamplePermId} of the newly registered sample; null if registration fails
   */
  public SamplePermId createSample(String code, String type, ExperimentIdentifier experimentIdentifier,
                                   Map<String, String> properties) {
    ensureLoggedIn();

    try {
      SampleCreation sample = prepareSampleCreation(code, type, experimentIdentifier, properties);

      return v3.createSamples(sessionToken, Collections.singletonList(sample)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create sample. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createSample(\"%s\", \"%s\", \"%s\", \"%s\") returned null.",
                                code, type, experimentIdentifier, properties));
      return null;
    }
  }

  /**
   * Create a new sample in the openBIS instance under the provided space, project and experiment code.
   * @param code Unique experiment code within space of creation
   * @param type Sample type that exists in the openBIS instance
   * @param space Code of space that exists in the openBIS instance
   * @param project Code of project that exists in the openBIS instance
   * @param experiment Code of experiment that exists in the openBIS instance
   * @param parents List of {@link SampleIdentifier} that exist in the openBIS instance
   * @param properties Map with name of existing PropertyType as key and its value
   * @return {@link SamplePermId} of the newly registered sample; null if registration fails
   */
  public SamplePermId createSample(String code, String type, String space, String project, String experiment,
                                   List<SampleIdentifier> parents, Map<String, String> properties) {
    ensureLoggedIn();

    try {
      SampleCreation sample = prepareSampleCreation(code, type, space, project, experiment, parents, properties);

      return v3.createSamples(sessionToken, Collections.singletonList(sample)).get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not create sample. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("createSample(\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\") returned null.",
                                code, type, space, project, experiment, parents, properties));
      return null;
    }
  }

  /**
   * Create multiple new samples in the openBIS instance.
   * @param samples List of {@link SampleCreation}
   * @return List of {@link SamplePermId} of the newly registered samples; null if registration fails
   */
  public List<SamplePermId> createSamples(List<SampleCreation> samples) {
    ensureLoggedIn();

    try {
      return v3.createSamples(sessionToken, samples);

    } catch (UserFailureException ufe) {
      logger.error("Could not create samples. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn("createSamples(List<SampleCreation>) returned null.");
      return null;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Misc ------------------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  @Override
  public List<PropertyType> listPropertiesForType(IEntityType type) {
    ensureLoggedIn();

    try {
      List<PropertyType> propertyTypes = new ArrayList<>();
      PropertyAssignmentSearchCriteria pasc = new PropertyAssignmentSearchCriteria();
      pasc.withIds().thatIn(type.getPropertyAssignments().stream().map(PropertyAssignment::getPermId).collect(Collectors.toList()));

      SearchResult<PropertyAssignment> propertyAssignments =
              v3.searchPropertyAssignments(sessionToken, pasc, fetchPropertyAssignmentWithPropertyType());

      if (propertyAssignments.getObjects().isEmpty()) {
        logger.info(String.format("No property assignments found with listPropertiesForType(%s).", type));
        return propertyTypes;
      }

      return propertyAssignments.getObjects().stream().map(PropertyAssignment::getPropertyType).collect(Collectors.toList());

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch property assignments. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("listPropertiesForType(%s) returned null.", type));
      return null;
    }
  }

  /**
   * Get all user names of users directly allocated to the provided openBIS space code.
   * @param spaceCode Code of the openBIS space
   * @return Set of openBIS user names
   */
  @Override
  public Set<String> getSpaceMembers(String spaceCode) {
    ensureLoggedIn();

    try {
      Set<String> members = new HashSet<>();
      RoleAssignmentSearchCriteria rasc = new RoleAssignmentSearchCriteria();
      rasc.withSpace().withCode().thatEquals(spaceCode);

      SearchResult<RoleAssignment> roleAssignments = v3.searchRoleAssignments(sessionToken, rasc, fetchRoleAssignmentWithSpaceAndUser());

      if (roleAssignments.getObjects().isEmpty()) { return members; }

      for (RoleAssignment ra : roleAssignments.getObjects()) {
        try {
          if (ra.getSpace().getCode().equals(spaceCode)) {
            members.add(ra.getUser().getUserId());
          }

        } catch (NotFetchedException nfe) {
          System.out.printf("RoleAssignment %s has no fetched space or user.", nfe.getMessage());
        }
      }

      return members;

    }  catch (UserFailureException ufe) {
      logger.error("Could not fetch role assignments. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("getSpaceMembers(\"%s\") returned null.", spaceCode));
      return null;
    }
  }

  /**
   * Trigger custom Application Server service registered in openBIS.
   * @param serviceName Name of the custom service which should be triggered
   * @param parameters Map with needed information for registration process
   * @return Object which is returned by the aggregation service
   */
  @Override
  public String triggerIngestionService(String serviceName, Map<String, Object> parameters) {
    ensureLoggedIn();

    try {
      CustomASServiceCode cassc = new CustomASServiceCode(serviceName);                // openBIS object for custom service code
      CustomASServiceExecutionOptions casseo = new CustomASServiceExecutionOptions();  // openBIS object for service parameter

      for (Map.Entry<String, Object> entry : parameters.entrySet()) {
        casseo.withParameter(entry.getKey(), entry.getValue());
      }

      Object result = v3.executeCustomASService(sessionToken, cassc, casseo);

      return result.toString();

    } catch (UserFailureException ufe) {
      logger.error("Could not execute custom AS service. Currently logged in user has sufficient permissions in openBIS?");
      logger.warn(String.format("triggerIngestionService(\"%s\", %s) returned null.", serviceName, parameters));
      return null;
    }
  }

  /**
   * Generates a barcode String depending on the provided project identifier.
   * @param projectIdentifier Identifier of the project
   * @param number_of_samples_offset Not used.
   * @return Project barcode
   */
  @Override
  public String generateBarcode(String projectIdentifier, int number_of_samples_offset) {
    Project project = getProjectByIdentifier(projectIdentifier);

    int numberOfSamples = getSamplesOfProject(project.getCode()).size();

    String barcode = project.getCode() + String.format("%03d", (numberOfSamples + 1)) + "S";
    barcode += checksum(barcode);

    return barcode;
  }

  /**
   * Transform openBIS entity type to human readable text.
   * Performs String replacement and does not query openBIS!
   * @param entityCode Entity code as string
   * @return Entity code as string in human readable text
   */
  @Override
  public String openbisCodeToString(String entityCode) {
    entityCode = WordUtils.capitalizeFully(entityCode.replace("_", " ").toLowerCase());

    String edit_string = entityCode
            .replace("Ngs", "NGS")
            .replace("Hla", "HLA")
            .replace("Rna", "RNA")
            .replace("Dna", "DNA")
            .replace("Ms", "MS");

    if (edit_string.startsWith("Q ")) {
      edit_string = edit_string.replace("Q ", "");
    }

    return edit_string;
  }

  /**
   * Function to get the download url for a file stored in the openBIS datastore server. Note that
   * this method does no checks, whether datasetcode or openbisFilename do exist. Deprecated: Use
   * getUrlForDataset() instead
   *
   * @param dataSetCode code of the openBIS dataset
   * @param openbisFilename name of the file stored in the given dataset
   * @return URL object of the download url for the given file
   * @throws MalformedURLException Returns an download url for the openbis dataset with the given
   *         code and dataset_type. Throughs MalformedURLException if a url can not be created from
   *         the given parameters. NOTE: datastoreURL differs from serverURL only by the port ->
   *         quick hack used
   */
  @Override
  public URL getDataStoreDownloadURL(String dataSetCode, String openbisFilename)
      throws MalformedURLException {
    String base = this.serverUrl.split(".de")[0] + ".de";
    String downloadURL = base + ":444";
    downloadURL += "/datastore_server/";

    downloadURL += dataSetCode;
    downloadURL += "/original/";
    downloadURL += openbisFilename;
    downloadURL += "?mode=simpleHtml&sessionID=";
    downloadURL += this.getSessionToken();
    return new URL(downloadURL);
  }

  @Override
  public URL getDataStoreDownloadURLLessGeneric(String dataSetCode, String openbisFilename)
      throws MalformedURLException {
    String base = this.serverUrl.split(".de")[0] + ".de";
    String downloadURL = base + ":444";
    downloadURL += "/datastore_server/";

    downloadURL += dataSetCode;
    downloadURL += "/";
    downloadURL += openbisFilename;
    downloadURL += "?mode=simpleHtml&sessionID=";
    downloadURL += this.getSessionToken();
    return new URL(downloadURL);
  }

  @Override
  public Map<Sample, List<Sample>> getParentMap(List<Sample> samples) {
    //Note: Samples must have fetched parents!
    Map<Sample, List<Sample>> parentMap = new HashMap<>();
    for (Sample sample : samples) {
      parentMap.put(sample, sample.getParents());
    }

    return parentMap;
  }

  /**
   * Returns lines of a spreadsheet of humanly readable information of the samples in a project.
   * Only one requested layer of the data model is returned. Experimental factors are returned in
   * the properties xml format and should be parsed before the spreadsheet is presented to the user.
   *
   * @param projectCode The 5 letter QBiC code of the project
   * @param sampleType The openBIS sampleType that should be included in the result
   */
  @Override
  public List<String> getProjectTSV(String projectCode, String sampleType) {
    return null;
  }

  @Override
  public List<Sample> getChildrenSamples(Sample sample) {
    //Note: Unnecessary method in v3 api
    return sample.getChildren();
  }

  /**
   * Compute status of project by checking status of the contained experiments
   *
   * @param project the Project object
   * @return ratio of finished experiments in this project
   */
  @Override
  public float computeProjectStatus(Project project) {
    float finishedExperiments = 0f;

    List<Experiment> experiments = this.getExperimentsOfProjectByCode(project.getCode());
    float numberExperiments = experiments.size();

    for (Experiment e : experiments) {
      if (e.getProperties().containsKey("Q_CURRENT_STATUS")) {
        if (e.getProperties().get("Q_CURRENT_STATUS").equals("FINISHED")) {
          finishedExperiments += 1.0;
        }
      }
    }

    return (numberExperiments > 0) ? finishedExperiments / experiments.size() : 0f;
  }

  /**
   * Compute status of project by checking status of the contained experiments Note: There is no
   * check whether the given experiments really belong to one project. You have to enusre that
   * yourself
   *
   * @param experiments list of experiments of a project.
   * @return ratio of finished experiments in this project
   */
  @Override
  public float computeProjectStatus(List<Experiment> experiments) {
    float finishedExperiments = 0f;
    float numberExperiments = experiments.size();

    for (Experiment e : experiments) {
      if (e.getProperties().containsKey("Q_CURRENT_STATUS")) {
        if (e.getProperties().get("Q_CURRENT_STATUS").equals("FINISHED")) {
          finishedExperiments += 1.0;
        }
      }
    }

    return (numberExperiments > 0) ? finishedExperiments / experiments.size() : 0f;
  }

  @Override
  public List<Experiment> listExperimentsOfProjects(List<Project> projectList) {
    ExperimentSearchCriteria sc = new ExperimentSearchCriteria();
    for (Project project : projectList) {
      sc.withProject().withCode().thatEquals(project.getCode());
    }
    SearchResult<Experiment> experiments =
        v3.searchExperiments(sessionToken, sc, fetchExperimentsCompletely());

    if (experiments.getObjects().isEmpty()) {
      return null;
    } else {
      return experiments.getObjects();
    }
  }

  @Override
  public List<Sample> listSamplesForProjects(List<String> projectIdentifiers) {
    return null;
  }

  /**
   * Retrieve datastore download url of dataset
   *
   * @param datasetCode Code of dataset
   * @param datasetName File name of dataset
   * @return URL to datastore location
   */
  @Override
  public URL getUrlForDataset(String datasetCode, String datasetName) throws MalformedURLException {
    return getDataStoreDownloadURL(datasetCode, datasetName);
  }

  /**
   * Retrieve inputstream for dataset
   *
   * @param datasetCode Code of dataset
   * @return input stream for dataset
   */
  @Override
  public InputStream getDatasetStream(String datasetCode) {
    return null;
  }

  /**
   * Retrieve inputstream for dataset in folder
   *
   * @param datasetCode Code of dataset
   * @param folder Folder of dataset
   * @return input stream of datasets
   */
  @Override
  public InputStream getDatasetStream(String datasetCode, String folder) {
    return null;
  }

}
