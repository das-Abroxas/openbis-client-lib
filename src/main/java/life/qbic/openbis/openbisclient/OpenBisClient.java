package life.qbic.openbis.openbisclient;

import static life.qbic.openbis.openbisclient.helper.OpenBisClientHelper.*;

import ch.ethz.sis.openbis.generic.asapi.v3.IApplicationServerApi;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.attachment.Attachment;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.common.search.SearchResult;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.dataset.DataSet;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.dataset.search.DataSetSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.Experiment;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.ExperimentType;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.fetchoptions.ExperimentFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.id.ExperimentIdentifier;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.search.ExperimentSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.search.ExperimentTypeSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.person.Person;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.person.search.PersonSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.project.Project;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.project.fetchoptions.ProjectFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.project.id.ProjectIdentifier;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.project.search.ProjectSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.property.PropertyType;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.roleassignment.Role;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.roleassignment.RoleAssignment;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.roleassignment.RoleLevel;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.Sample;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.SampleType;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.fetchoptions.SampleFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.id.SampleIdentifier;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.search.SampleSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.search.SampleTypeSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.space.Space;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.space.fetchoptions.SpaceFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.space.search.SpaceSearchCriteria;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.vocabulary.Vocabulary;
import ch.ethz.sis.openbis.generic.asapi.v3.exceptions.NotFetchedException;
import ch.ethz.sis.openbis.generic.dssapi.v3.IDataStoreServerApi;
import ch.systemsx.cisd.common.exceptions.NotImplementedException;
import ch.systemsx.cisd.common.exceptions.UserFailureException;
import ch.systemsx.cisd.common.spring.HttpInvokerUtils;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The type Open bis client.
 */
public class OpenBisClient implements IOpenBisClient {

  private final int TIMEOUT = 100000;
  private final String userId, password, serverUrl, asApiUrl, dsApiUrl;
  private final IApplicationServerApi v3;
  private final IDataStoreServerApi dss3;
  private static final Logger logger = LogManager.getLogger(OpenBisClient.class);

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
   * @param i the integer value which should be mapped
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
   * Logs in to the openBIS server with the user provided at instantiation of this class
   *   and invalidates current session token if present.
   */
  @Override
  public void login() {
    if (loggedin()) {
      logout();
    }
    // Login to obtain a session token
    sessionToken = v3.login(userId, password);
  }

  /**
   * Logs into openBIS as the user of the provided username after logout if session of other user is still active.
   * A user with the provided username has to exist in the openBIS server.
   * @param user openBIS user name
   */
  public void loginAsUser(String user) {
    if (loggedin()) {
      logout();
    }

    // Login to obtain a session token
    sessionToken = v3.loginAs(userId, password, user);
  }

  /**
   * Checks if issued session token is still active in openBIS.
   * Session tokens are issued with {@link #login() login} and {@link #loginAsUser(String) loginAsUser}.
   * @return True if session token is still active else false
   */
  @Override
  public boolean loggedin() {
    try {
      return v3.isSessionActive(sessionToken);
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Checks if session token for provided openBIS user is still active in openBIS.
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
   * Checks if issued session token is still active in openBIS.
   * If not, one attempt is made to reconnect to the openBIS server with the user provided at instantiation of this class.
   */
  @Override
  public void ensureLoggedIn() {
    if (!this.loggedin()) {
      this.login();
    }
  }

  /**
   * Checks if issued session token is still active in openBIS.
   * If not, one attempt is made to reconnect to the openBIS server with the username provided.
   */
  public void ensureLoggedIn(String user) {
    if (!this.loggedIn(userId)) {
      this.loginAsUser(userId);
    }
  }

  /**
   * Logs out of the openBIS server and invalidates the issued session token.
   */
  @Override
  public void logout() {
    if (loggedin()) {
      v3.logout(sessionToken);
    }
    sessionToken = null;
  }

  /**
   * Returns whether a user is instance admin in openBIS.
   * @param user openBIS user name
   * @return true, if user is instance admin, false otherwise
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
   * Function to get a list of all space identifiers which are registered in this openBIS instance
   * @return list with the identifiers of all available spaces
   */
  @Override
  public List<String> listSpaces() {
    ensureLoggedIn();

    try {
      SearchResult<Space> spaces = v3.searchSpaces(sessionToken, new SpaceSearchCriteria(), new SpaceFetchOptions());
      List<String> spaceIdentifiers = new ArrayList<>();

      for (Space space : spaces.getObjects()) {
        spaceIdentifiers.add(space.getCode());
      }

      return spaceIdentifiers;

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch spaces from openBIS. Is currently logged in user admin?");
      logger.warn("listSpaces() returned null.");
      return null;
    }
  }

  /**
   * Returns Space names a given user should be able to see
   *
   * @param userID Username found in openBIS
   * @return List of space names with projects this user has access to
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
   * Get all space codes which are available for the provided openBIS user.
   * @param user openBIS user name
   * @return List with codes of all available spaces for specific openBIS user
   */
  public List<String> listSpacesForUser(String user) {
    ensureLoggedIn(user);

    try {
      SearchResult<Space> spaces = v3.searchSpaces(sessionToken, new SpaceSearchCriteria(), fetchSpacesCompletely());
      List<String> spaceIdentifiers = spaces.getObjects().stream().map(Space::getCode).collect(Collectors.toList());

      return spaceIdentifiers;

    } catch (UserFailureException ufe) {
      logger.error(String.format("User %s could not fetch spaces.", user));
      logger.warn("Returned empty list.");
      return null;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Projects --------------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Function to get all projects which are registered in this openBIS instance
   *
   * @return list with all projects which are registered in this openBIS instance
   */
  @Override
  public List<Project> listProjects() {
    ensureLoggedIn();

    try {
      SearchResult<Project> projects = v3.searchProjects(sessionToken, new ProjectSearchCriteria(), fetchProjectsCompletely());
      return projects.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch projects from openBIS. Is currently logged in user admin?");
      logger.warn("listProjects() returned null.");
      return null;
    }
  }

  /**
   * Get all projects which are available to the provided user in this openBIS instance
   * @param user openBIS user name
   * @return List with all available projects for specific openBIS user
   */
  public List<Project> listProjectsForUser(String user) {
    ensureLoggedIn(user);

    try {
      SearchResult<Project> projects = v3.searchProjects(sessionToken, new ProjectSearchCriteria(), fetchProjectsCompletely());
      return projects.getObjects();

    } catch (UserFailureException ufe) {
      logger.error(String.format("Could not fetch projects of user %s. Does this user exist in openBIS?", user));
      logger.warn("listProjectsForUser(String user) returned null.");
      return null;
    }
  }

  @Override
  public List<Project> getProjectsOfSpace(String space) {
    ensureLoggedIn();

    try {
      ProjectSearchCriteria sc = new ProjectSearchCriteria();
      sc.withSpace().withCode().thatEquals(space);

      SearchResult<Project> projects = v3.searchProjects(sessionToken, sc, fetchProjectsCompletely());

      return projects.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch projects of space. Does the currently logged in user have sufficient permissions in openBIS?");
      logger.warn("getProjectsOfSpace(String space) returned null.");
      return null;
    }
  }

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

  @Override
  public Project getProjectOfExperimentByIdentifier(String experimentIdentifier) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria sc = new ExperimentSearchCriteria();
      sc.withOrOperator();
      sc.withCode().thatEquals(experimentIdentifier);
      sc.withId().thatEquals(new ExperimentIdentifier(experimentIdentifier));

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, sc, fetchExperimentsCompletely());

      return experiments.getObjects().isEmpty() ? null : experiments.getObjects().get(0).getProject();

    } catch (NotFetchedException nfe) {
      logger.error("Experiment has no fetched project.");
      logger.warn("getProjectOfExperimentByIdentifier(String experimentIdentifier) returned null.");
      return null;

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiment. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getProjectOfExperimentByIdentifier(String experimentIdentifier) returned null.");
      return null;
    }
  }

  @Override
  public Project getProjectByIdentifier(String projectIdentifier) {
    // ToDo: Same output as getProjectByCode(String projectCode). These methods should be merged.
    ensureLoggedIn();

    try {
      ProjectSearchCriteria sc = new ProjectSearchCriteria();
      sc.withOrOperator();
      sc.withId().thatEquals(new ProjectIdentifier(projectIdentifier));
      sc.withCode().thatEquals(projectIdentifier);

      SearchResult<Project> projects = v3.searchProjects(sessionToken, sc, fetchProjectsCompletely());

      return projects.getObjects().isEmpty() ? null : projects.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch project. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getProjectByIdentifier(String projectIdentifier) returned null.");
      return null;
    }
  }

  @Override
  public Project getProjectByCode(String projectCode) {
    // ToDo: Same output as getProjectByIdentifier(String projectIdentifier). These methods should be merged.
    ensureLoggedIn();

    try {
      ProjectSearchCriteria sc = new ProjectSearchCriteria();
      sc.withOrOperator();
      sc.withCode().thatEquals(projectCode);
      sc.withId().thatEquals(new ProjectIdentifier(projectCode));

      SearchResult<Project> projects = v3.searchProjects(sessionToken, sc, fetchProjectsCompletely());

      return projects.getObjects().isEmpty() ? null : projects.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch project. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getProjectByIdentifier(String projectIdentifier) returned null.");
      return null;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Experiment / ExperimentType -------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Function to list all Experiments which are registered in the openBIS instance.
   *
   * @return list with all experiments registered in this openBIS instance
   */
  @Override
  public List<Experiment> listExperiments() {
    ensureLoggedIn();

    try {
      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, new ExperimentSearchCriteria(), fetchExperimentsCompletely());
      return experiments.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments from openBIS. Is currently logged in user admin?");
      logger.warn("listExperiments() returned null.");
      return null;
    }
  }

  /**
   * Get all Experiments which are available to the provided user in this openBIS instance
   * @param user openBIS user name
   * @return List with all available experiments for specific openBIS user
   */
  public List<Experiment> listExperimentsForUser(String user) {
    ensureLoggedIn(user);

    try {
      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, new ExperimentSearchCriteria(), fetchExperimentsCompletely());
      return experiments.getObjects();

    } catch (UserFailureException ufe) {
      logger.error(String.format("Could not fetch experiments of user %s. Does this user exist in openBIS?", user));
      logger.warn("listExperimentsForUser(String user) returned null.");
      return null;
    }
  }

  /**
   * Returns a list of all Experiments of a certain user.
   *
   * @param userID ID of user
   * @return A list containing the experiments
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

  @Override
  public List<Experiment> getExperimentsOfSpace(String spaceIdentifier) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria sc = new ExperimentSearchCriteria();
      sc.withProject().withSpace().withCode().thatEquals(spaceIdentifier);

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, sc, fetchExperimentsCompletely());

      return experiments.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments from openBIS. Is currently logged in user admin?");
      logger.warn("getExperimentsOfSpace(String spaceIdentifier) returned null.");
      return null;
    }
  }

  @Override
  public List<Experiment> getExperimentsOfProjectByIdentifier(String projectIdentifier) {
    // ToDo: Can be removed from IOpenBisClient as getExperimentsForProject(String) is sufficient

    return getExperimentsForProject(projectIdentifier);  // ensureLoggedIn() is called in getExperimentsForProject
  }

  @Override
  public List<Experiment> getExperimentsOfProjectByCode(String projectCode) {
    // ToDo: Can be removed from IOpenBisClient as getExperimentsForProject(String) is sufficient

    return getExperimentsForProject(projectCode);  // ensureLoggedIn() is called in getExperimentsForProject
  }

  /**
   * Function to list all Experiments for a specific project which are registered in the openBIS
   * instance. av: 19353 ms
   *
   * @param project the project for which the experiments should be listed
   * @return list with all experiments registered in this openBIS instance
   */
  @Override
  public List<Experiment> getExperimentsForProject(Project project) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria sc = new ExperimentSearchCriteria();
      sc.withProject().withCode().thatEquals(project.getCode());

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, sc, fetchExperimentsCompletely());

      return experiments.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments from openBIS. Is currently logged in user admin?");
      logger.warn("getExperimentsForProject(Project project) returned null.");
      return null;
    }
  }

  /**
   * Function to list all Experiments for a specific project which are registered in the openBIS
   * instance.
   *
   * @param projectCodeOrIdentifier Project code or identifer as defined by openBIS.
   * @return list with all experiments registered in this openBIS instance
   */
  @Override
  public List<Experiment> getExperimentsForProject(String projectCodeOrIdentifier) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria sc = new ExperimentSearchCriteria();
      sc.withOrOperator();
      sc.withProject().withCode().thatEquals(projectCodeOrIdentifier);
      sc.withProject().withId().thatEquals(new ProjectIdentifier(projectCodeOrIdentifier));

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, sc, fetchExperimentsCompletely());

      return experiments.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments from openBIS. Is currently logged in user admin?");
      logger.warn("getExperimentsForProject(String projectCodeOrIdentifier) returned null.");
      return null;
    }
  }

  public Experiment getExperiment(String experimentCodeOrIdentifier) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria sc = new ExperimentSearchCriteria();
      sc.withCode().thatEquals(experimentCodeOrIdentifier);
      sc.withId().thatEquals( new ExperimentIdentifier(experimentCodeOrIdentifier) );

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, sc, fetchExperimentsCompletely());

      return experiments.getObjects().isEmpty() ? null : experiments.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments from openBIS. Is currently logged in user admin?");
      logger.warn("getExperimentByCode(String experimentCode) returned null.");
      return null;
    }
  }

  @Override
  public Experiment getExperimentByCode(String experimentCode) {
    // ToDo: Can be removed from IOpenBisClient as getExperiment(String) is sufficient

    return getExperiment(experimentCode);  // ensureLoggedIn() is called in getExperiment
  }

  @Override
  public Experiment getExperimentById(String experimentId) {
    // ToDo: Can be removed from IOpenBisClient as getExperiment(String) is sufficient

    return getExperiment(experimentId);  // ensureLoggedIn() is called in getExperiment
  }

  @Override
  public List<Experiment> getExperimentsOfType(String type) {
    ensureLoggedIn();

    try {
      ExperimentSearchCriteria sc = new ExperimentSearchCriteria();
      sc.withType().withCode().thatEquals(type);

      SearchResult<Experiment> experiments = v3.searchExperiments(sessionToken, sc, fetchExperimentsCompletely());

      return experiments.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiments from openBIS. Is currently logged in user admin?");
      logger.warn("getExperimentsOfType(String type) returned null.");
      return null;
    }
  }

  /**
   * Function to get a ExperimentType object of a experiment type
   *
   * @param experimentType the experiment type as string
   * @return the ExperimentType object of the corresponding experiment type
   */
  @Override
  public ExperimentType getExperimentTypeByString(String experimentType) {
    ensureLoggedIn();

    try {
      ExperimentTypeSearchCriteria sc = new ExperimentTypeSearchCriteria();
      sc.withCode().thatContains(experimentType);

      SearchResult<ExperimentType> experimentTypes = v3.searchExperimentTypes(sessionToken, sc, fetchExperimentTypesCompletely());

      return experimentTypes.getObjects().isEmpty() ? null : experimentTypes.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch experiment types from openBIS. Is currently logged in user admin?");
      logger.warn("getExperimentTypeByString(String experimentType) returned null.");
      return null;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Sample / SampleType ---------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Get all Samples which are registered in the openBIS instance.
   * @return List of openBIS v3 Sample
   */
  public List<Sample> listSamples() {
    ensureLoggedIn();

    try {
      SearchResult<Sample> samples = v3.searchSamples(sessionToken, new SampleSearchCriteria(), fetchSamplesCompletely());
      return samples.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples from openBIS. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("listSamples() returned null.");
      return null;
    }
  }

  /**
   * Get all Samples which are available to the provided user in this openBIS instance
   * @param user openBIS user name
   * @return List with all available samples for specific openBIS user
   */
  public List<Sample> listSamplesForUser(String user) {
    ensureLoggedIn(user);

    try {
      SearchResult<Sample> samples = v3.searchSamples(sessionToken, new SampleSearchCriteria(), fetchSamplesCompletely());
      return samples.getObjects();

    } catch (UserFailureException ufe) {
      logger.error(String.format("Could not fetch samples of user %s. Does this user exist in openBIS?", user));
      logger.warn("listSamplesForUser(String user) returned null.");
      return null;
    }
  }

  /**
   * Function to retrieve all samples of a given space
   *
   * @param spaceIdentifier identifier of the openBIS space
   * @return list with all samples of the given space
   */
  @Override
  public List<Sample> getSamplesofSpace(String spaceIdentifier) {
    ensureLoggedIn();

    try {
      SampleSearchCriteria sampleSearchCriteria = new SampleSearchCriteria();
      sampleSearchCriteria.withSpace().withCode().thatEquals(spaceIdentifier);

      SearchResult<Sample> samplesOfExperiment = v3.searchSamples(sessionToken, sampleSearchCriteria, fetchSamplesCompletely());
      return samplesOfExperiment.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getSamplesofSpace(String spaceIdentifier) returned null.");
      return null;
    }
  }

  @Override
  public List<Sample> getSamplesOfProject(String projIdentifier) {
    ensureLoggedIn();

    try {
      SampleSearchCriteria sampleSearchCriteria = new SampleSearchCriteria();
      sampleSearchCriteria.withOrOperator();
      sampleSearchCriteria.withExperiment().withProject().withCode().thatEquals(projIdentifier);
      sampleSearchCriteria.withExperiment().withProject().withId().thatEquals(new ProjectIdentifier(projIdentifier));

      SearchResult<Sample> samples = v3.searchSamples(sessionToken, sampleSearchCriteria, fetchSamplesCompletely());

      return samples.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getSamplesOfProject(String projIdentifier) returned null.");
      return null;
    }
  }

  /**
   * Function to retrieve all samples of a given experiment Note: seems to throw a
   * ch.systemsx.cisd.common.exceptions.UserFailureException if wrong identifier given TODO Should
   * we catch it and throw an illegalargumentexception instead? would be a lot clearer in my opinion
   *
   * @param experimentIdentifier identifier/code (both should work) of the openBIS experiment
   * @return list with all samples of the given experiment
   */
  @Override
  public List<Sample> getSamplesofExperiment(String experimentIdentifier) {
    ensureLoggedIn();

    try {
      SampleSearchCriteria sampleSearchCriteria = new SampleSearchCriteria();
      sampleSearchCriteria.withOrOperator();
      sampleSearchCriteria.withExperiment().withCode().thatEquals(experimentIdentifier);
      sampleSearchCriteria.withExperiment().withId().thatEquals( new ExperimentIdentifier(experimentIdentifier) );

      SearchResult<Sample> samplesOfExperiment = v3.searchSamples(sessionToken, sampleSearchCriteria, fetchSamplesCompletely());
      return samplesOfExperiment.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getSamplesOfProject(String projIdentifier) returned null.");
      return null;
    }
  }

  public Sample getSample(String sampleCodeOrIdentifier) {
    ensureLoggedIn();

    try {
      SampleSearchCriteria sampleSearchCriteria = new SampleSearchCriteria();
      sampleSearchCriteria.withOrOperator();
      sampleSearchCriteria.withCode().thatEquals(sampleCodeOrIdentifier);
      sampleSearchCriteria.withId().thatEquals(new SampleIdentifier(sampleCodeOrIdentifier));

      SearchResult<Sample> samples = v3.searchSamples(sessionToken, sampleSearchCriteria, fetchSamplesCompletely());

      return samples.getObjects().isEmpty() ? null : samples.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getSample(String sampleCodeOrIdentifier) returned null.");
      return null;
    }
  }

  @Override
  public Sample getSampleByIdentifier(String sampleIdentifier) {
    // ToDo: Can be removed from IOpenBisClient as getSample(String) is sufficient

    return getSample(sampleIdentifier);  // ensureLoggedIn() is called in getSample
  }

  /**
   * Function to get a sample with its parents and children
   *
   * @param sampCode code of the openBIS sample
   * @return sample
   */
  @Override
  public List<Sample> getSamplesWithParentsAndChildren(String sampCode) {
    // ToDo: Unclear if parents and children should be fetched or directly included into the list.
    ensureLoggedIn();

    try {
      SampleSearchCriteria sampleSearchCriteria = new SampleSearchCriteria();
      sampleSearchCriteria.withCode().thatEquals(sampCode);

      SampleFetchOptions sampleFetchOptions = fetchSamplesCompletely();
      sampleFetchOptions.withChildrenUsing(fetchSamplesCompletely());
      sampleFetchOptions.withParentsUsing(fetchSamplesCompletely());

      SearchResult<Sample> samples = v3.searchSamples(sessionToken, sampleSearchCriteria, fetchSamplesCompletely());

      return samples.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getSamplesWithParentsAndChildren(String sampCode) returned null.");
      return null;
    }
  }

  @Override
  public List<Sample> getSamplesOfType(String type) {
    ensureLoggedIn();

    try {
      SampleSearchCriteria sampleSearchCriteria = new SampleSearchCriteria();
      sampleSearchCriteria.withType().withCode().thatEquals(type);

      SearchResult<Sample> samples = v3.searchSamples(sessionToken, sampleSearchCriteria, fetchSamplesCompletely());
      return samples.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch samples. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getSamplesWithParentsAndChildren(String sampCode) returned null.");
      return null;
    }
  }

  @Override
  public SampleType getSampleTypeByString(String sampleType) {
    ensureLoggedIn();

    try {
      SampleTypeSearchCriteria sc = new SampleTypeSearchCriteria();
      sc.withCode().thatEquals(sampleType);

      SearchResult<SampleType> sampleTypes = v3.searchSampleTypes(sessionToken, sc, fetchSampleTypesCompletely());

     return sampleTypes.getObjects().isEmpty() ? null : sampleTypes.getObjects().get(0);

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch sample types. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getSampleTypeByString(String sampleType) returned null.");
      return null;
    }
  }

  /**
   * Function to retrieve a map with sample type code as key and the sample type object as value
   *
   * @return map with sample types
   */
  @Override
  public Map<String, SampleType> getSampleTypes() {
    ensureLoggedIn();

    try {
      SearchResult<SampleType> sampleTypes = v3.searchSampleTypes(sessionToken, new SampleTypeSearchCriteria(), fetchSampleTypesCompletely());

      Map<String, SampleType> types = new HashMap<>();
      for (SampleType t : sampleTypes.getObjects()) {
        types.put(t.getCode(), t);
      }

      return types;

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch sample types. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getSampleTypes() returned null.");
      return null;
    }
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- DataSets --------------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  /**
   * Get all DataSets which are registered in the openBIS instance.
   * @return List of openBIS v3 DataSet
   */
  public List<DataSet> listDatasets() {
    ensureLoggedIn();

    try {
      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, new DataSetSearchCriteria(), fetchDataSetsCompletely());
      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getSampleDatasets() returned null.");
      return null;
    }
  }

  /**
   * Get all DataSets which are available to the provided user in this openBIS instance
   * @param user openBIS user name
   * @return List with all available datasets for specific openBIS user
   */
  public List<DataSet> listDatasetsForUser(String user) {
    ensureLoggedIn(user);

    try {
      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, new DataSetSearchCriteria(), fetchDataSetsCompletely());
      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error(String.format("Could not fetch datasets of user %s. Does this user exist in openBIS?", user));
      logger.warn("Returned empty list.");
      return null;
    }
  }

  /**
   * Function to list all datasets of a specific openBIS space
   *
   * @param spaceIdentifier identifier of the openBIS space
   * @return list with all datasets of the given space
   */
  @Override
  public List<DataSet> getDataSetsOfSpaceByIdentifier(String spaceIdentifier) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria sc = new DataSetSearchCriteria();
      sc.withSample().withSpace().withCode().thatEquals(spaceIdentifier);

      SearchResult<DataSet> dataSets = v3.searchDataSets(sessionToken, sc, fetchDataSetsCompletely());

      return dataSets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getDataSetsOfSpaceByIdentifier(String spaceIdentifier) returned null.");
      return null;
    }
  }

  /**
   * Function to list all datasets of a specific openBIS project
   *
   * @param projects openBIS v3 Project objects
   * @return list with all datasets of the given project
   */
  @Override
  public List<DataSet> getDataSetsOfProjects(List<Project> projects) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria dsc = new DataSetSearchCriteria();
      dsc.withSample().withProject().withCodes().thatIn( projects.stream().map(Project::getCode).collect(Collectors.toList()) );

      SearchResult<DataSet> datasets = v3.searchDataSets(sessionToken, new DataSetSearchCriteria(), fetchDataSetsCompletely());
      return datasets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getDataSetsOfProjects(List<Project> projects) returned null.");
      return null;
    }
  }

  /**
   * Function to list all datasets of a specific openBIS project
   *
   * @param projectIdentifier identifier of the openBIS project
   * @return list with all datasets of the given project
   */
  @Override
  public List<DataSet> getDataSetsOfProjectByIdentifier(String projectIdentifier) {
    // TODO does not work yet
    return null;
  }

  /**
   * List all datasets for given experiment identifiers
   *
   * @param experimentIdentifiers list of experiment identifiers
   * @return List of datasets
   */
  @Override
  public List<DataSet> listDataSetsForExperiments(List<String> experimentIdentifiers) {
    return null;
  }

  /**
   * Returns all datasets of a given experiment. The new version should run smoother
   *
   * @param experimentIdentifier identifier or code of the openbis experiment
   * @return list of all datasets of the given experiment
   */
  @Override
  public List<DataSet> getDataSetsOfExperimentByIdentifier(String experimentIdentifier) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria sc = new DataSetSearchCriteria();
      sc.withExperiment().withId().thatEquals(new ExperimentIdentifier(experimentIdentifier));

      SearchResult<DataSet> dataSets = v3.searchDataSets(sessionToken, sc, fetchDataSetsCompletely());

      return dataSets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getDataSetsOfExperimentByIdentifier(String experimentIdentifier) returned null.");
      return null;
    }
  }

  /**
   * Function to list all datasets of a specific experiment (watch out there are different dataset
   * classes)
   *
   * @param experimentPermID permId of the openBIS experiment
   * @return list with all datasets of the given experiment
   */
  @Override
  public List<DataSet> getDataSetsOfExperiment(String experimentPermID) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria sc = new DataSetSearchCriteria();
      sc.withExperiment().withPermId().thatEquals(experimentPermID);

      SearchResult<DataSet> dataSets = v3.searchDataSets(sessionToken, sc, fetchDataSetsCompletely());

      return dataSets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getDataSetsOfExperiment(String experimentPermID) returned null.");
      return null;
    }
  }

  /**
   * List all datasets for given sample identifiers
   *
   * @param sampleIdentifier list of sample identifiers
   * @return List of datasets
   */
  @Override
  public List<DataSet> listDataSetsForSamples(List<String> sampleIdentifier) {
    return null;
  }

  /**
   * Function to list all datasets of a specific sample (watch out there are different dataset
   * classes)
   *
   * @param sampleCode code or identifier of the openBIS sample
   * @return list with all datasets of the given sample
   */
  @Override
  public List<DataSet> getDataSetsOfSample(String sampleCode) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria sc = new DataSetSearchCriteria();
      sc.withSample().withCode().thatEquals(sampleCode);

      SearchResult<DataSet> dataSets = v3.searchDataSets(sessionToken, sc, fetchDataSetsCompletely());

      return dataSets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getDataSetsOfSample(String sampleCode) returned null.");
      return null;
    }
  }

  /**
   * Function to list all datasets of a specific sample (watch out there are different dataset
   * classes)
   *
   * @param sampleIdentifier identifier of the openBIS sample
   * @return list with all datasets of the given sample
   */
  @Override
  public List<DataSet> getDataSetsOfSampleByIdentifier(String sampleIdentifier) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria sc = new DataSetSearchCriteria();
      sc.withOrOperator();
      sc.withSample().withId().thatEquals(new SampleIdentifier(sampleIdentifier));

      SearchResult<DataSet> dataSets = v3.searchDataSets(sessionToken, sc, fetchDataSetsCompletely());

      return dataSets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getDataSetsOfSampleByIdentifier(String sampleIdentifier) returned null.");
      return null;
    }
  }

  @Override
  public List<DataSet> getDataSetsByType(String type) {
    ensureLoggedIn();

    try {
      DataSetSearchCriteria sc = new DataSetSearchCriteria();
      sc.withType().withCode().thatEquals(type);

      SearchResult<DataSet> dataSets = v3.searchDataSets(sessionToken, sc, fetchDataSetsCompletely());

      return dataSets.getObjects();

    } catch (UserFailureException ufe) {
      logger.error("Could not fetch datasets. Has the currently logged in user sufficient permissions in openBIS?");
      logger.warn("getDataSetsByType(String type) returned null.");
      return null;
    }
  }




  @Override
  public List<Attachment> listAttachmentsForSampleByIdentifier(String sampleIdentifier) {
    ensureLoggedIn();

    return getSampleByIdentifier(sampleIdentifier).getAttachments();
  }

  @Override
  public List<Attachment> listAttachmentsForProjectByIdentifier(String projectIdentifier) {
    ensureLoggedIn();

    return getProjectByIdentifier(projectIdentifier).getAttachments();
  }

  /**
   * Returns all users of a Space.
   *
   * @param spaceCode code of the openBIS space
   * @return set of user names as string
   */
  @Override
  public Set<String> getSpaceMembers(String spaceCode) {
    // TODO cannot find an opportunity to do that
    return null;
  }

  /**
   * Function to list the vocabulary terms for a given property which has been added to openBIS. The
   * property has to be a Controlled Vocabulary Property.
   *
   * @param property the property type
   * @return list of the vocabulary terms of the given property
   */
  @Override
  public List<String> listVocabularyTermsForProperty(PropertyType property) {
    return null;

  }

  /**
   * Function to get the label of a CV item for some property
   *
   * @param propertyType the property type
   * @param propertyValue the property value
   * @return Label of CV item
   */
  @Override
  public String getCVLabelForProperty(PropertyType propertyType, String propertyValue) {
    return null;
  }

  /**
   * Function to trigger ingestion services registered in openBIS
   *
   * @param serviceName name of the ingestion service which should be triggered
   * @param parameters map with needed information for registration process
   * @return object name of the QueryTableModel which is returned by the aggregation service
   */
  @Override
  public String triggerIngestionService(String serviceName, Map<String, Object> parameters) {
    return null;
  }

  @Override
  public String generateBarcode(String proj, int number_of_samples_offset) {
    Project project = getProjectByIdentifier(proj);
    int numberOfSamples = getSamplesOfProject(project.getCode()).size();
    String barcode = project.getCode() + String.format("%03d", (numberOfSamples + 1)) + "S";
    barcode += checksum(barcode);

    return barcode;
  }

  /**
   * Function to transform openBIS entity type to human readable text. Performs String replacement
   * and does not query openBIS!
   *
   * @param entityCode the entity code as string
   * @return entity code as string in human readable text
   */
  @Override
  public String openBIScodeToString(String entityCode) {
    entityCode = WordUtils.capitalizeFully(entityCode.replace("_", " ").toLowerCase());
    String edit_string = entityCode.replace("Ngs", "NGS").replace("Hla", "HLA")
        .replace("Rna", "RNA").replace("Dna", "DNA").replace("Ms", "MS");
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
    // TODO samples must have fetched parents!
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
    // TODO unnecessary method in v3 api
    return sample.getChildren();
  }

  @Override
  public boolean spaceExists(String spaceCode) {
    return listSpaces().contains(spaceCode);
  }

  @Override
  public boolean projectExists(String spaceCode, String projectCode) {
    // TODO why do we need the space code here? Then the method should be named differently
    ProjectSearchCriteria sc = new ProjectSearchCriteria();
    sc.withSpace().withCode().thatEquals(spaceCode);
    sc.withCode().thatEquals(projectCode);
    SearchResult<Project> projects = v3.searchProjects(sessionToken, sc, new ProjectFetchOptions());

    return projectCode != null && projects.getTotalCount() != 0;
  }

  @Override
  public boolean expExists(String spaceCode, String projectCode, String experimentCode) {
    // TODO why do we need the space code here? Then the method should be named differently
    ExperimentSearchCriteria sc = new ExperimentSearchCriteria();
    sc.withProject().withSpace().withCode().thatEquals(spaceCode);
    sc.withCode().thatEquals(experimentCode);
    sc.withProject().withCode().thatEquals(projectCode);
    SearchResult<Experiment> experiments =
        v3.searchExperiments(sessionToken, sc, new ExperimentFetchOptions());
    return experiments.getTotalCount() != 0;
  }

  @Override
  public boolean sampleExists(String sampleCode) {
    return searchSampleByCode(sampleCode).size() > 0;
  }

  @Override
  public List<Sample> searchSampleByCode(String sampleCode) {
    SampleSearchCriteria sc = new SampleSearchCriteria();
    sc.withCode().thatEquals(sampleCode);
    SampleFetchOptions fetchOptions = new SampleFetchOptions();
    fetchOptions.withSpace();
    SearchResult<Sample> samples = v3.searchSamples(sessionToken, sc, fetchOptions);
    return samples.getObjects();
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
      if (e.getProperties().keySet().contains("Q_CURRENT_STATUS")) {
        if (e.getProperties().get("Q_CURRENT_STATUS").equals("FINISHED")) {
          finishedExperiments += 1.0;
        } ;
      }
    }
    if (numberExperiments > 0) {
      return finishedExperiments / experiments.size();
    } else {
      return 0f;
    }
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
      if (e.getProperties().keySet().contains("Q_CURRENT_STATUS")) {
        if (e.getProperties().get("Q_CURRENT_STATUS").equals("FINISHED")) {
          finishedExperiments += 1.0;
        } ;
      }
    }
    if (numberExperiments > 0) {
      return finishedExperiments / experiments.size();
    } else {
      return 0f;
    }
  }

  /**
   * Returns a map of Labels (keys) and Codes (values) in a Vocabulary in openBIS
   *
   * @param vocabularyCode Code of the Vocabulary type
   * @return A map containing the labels as keys and codes as values in String format
   */
  @Override
  public Map<String, String> getVocabCodesAndLabelsForVocab(String vocabularyCode) {
    return null;
  }

  @Override
  public Vocabulary getVocabulary(String vocabularyCode) {
    return null;
  }

  /**
   * Returns a list of all Codes in a Vocabulary in openBIS. This is useful when labels don't exist
   * or are not needed.
   *
   * @param vocabularyCode Code of the Vocabulary type
   * @return A list containing the codes of the vocabulary type
   */
  @Override
  public List<String> getVocabCodesForVocab(String vocabularyCode) {
    return null;
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
