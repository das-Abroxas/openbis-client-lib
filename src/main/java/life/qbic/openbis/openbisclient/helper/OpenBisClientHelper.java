package life.qbic.openbis.openbisclient.helper;

import ch.ethz.sis.openbis.generic.asapi.v3.dto.dataset.fetchoptions.DataSetFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.fetchoptions.ExperimentFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.experiment.fetchoptions.ExperimentTypeFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.person.fetchoptions.PersonFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.project.fetchoptions.ProjectFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.property.fetchoptions.PropertyAssignmentFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.property.fetchoptions.PropertyTypeFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.roleassignment.fetchoptions.RoleAssignmentFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.fetchoptions.SampleFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.fetchoptions.SampleTypeFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.space.fetchoptions.SpaceFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.vocabulary.fetchoptions.VocabularyFetchOptions;
import ch.ethz.sis.openbis.generic.asapi.v3.dto.vocabulary.fetchoptions.VocabularyTermFetchOptions;

/**
 * Class that only contains private static helper methods
 *   which are used by {@link life.qbic.openbis.openbisclient.OpenBisClient}.
 */
public class OpenBisClientHelper {

  private OpenBisClientHelper() {}

  /* ------------------------------------------------------------------------------------ */
  /* ----- Person / RoleAssignment ------------------------------------------------------ */
  /* ------------------------------------------------------------------------------------ */
  public static PersonFetchOptions fetchPersonCompletely() {
    PersonFetchOptions personFetchOptions = new PersonFetchOptions();
    personFetchOptions.withRegistrator();
    personFetchOptions.withSpace();
    personFetchOptions.withRoleAssignments();

    return personFetchOptions;
  }

  public static PersonFetchOptions fetchPersonWithSpace() {
    PersonFetchOptions personFetchOptions = new PersonFetchOptions();
    personFetchOptions.withSpaceUsing(fetchSpacesCompletely());

    return personFetchOptions;
  }

  public static RoleAssignmentFetchOptions fetchRoleAssignmentCompletely() {
    RoleAssignmentFetchOptions roleAssignmentFetchOptions = new RoleAssignmentFetchOptions();
    roleAssignmentFetchOptions.withRegistrator();
    roleAssignmentFetchOptions.withSpace();
    roleAssignmentFetchOptions.withProject();
    roleAssignmentFetchOptions.withUser();
    roleAssignmentFetchOptions.withAuthorizationGroup();

    return roleAssignmentFetchOptions;
  }

  public static RoleAssignmentFetchOptions fetchRoleAssignmentWithSpaceAndUser() {
    RoleAssignmentFetchOptions roleAssignmentFetchOptions = new RoleAssignmentFetchOptions();
    roleAssignmentFetchOptions.withUserUsing(fetchPersonCompletely());
    roleAssignmentFetchOptions.withSpaceUsing(fetchSpacesCompletely());

    return roleAssignmentFetchOptions;
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Space ------------------------------------------------------------------------ */
  /* ------------------------------------------------------------------------------------ */
  public static SpaceFetchOptions fetchSpacesCompletely() {
    SpaceFetchOptions spaceFetchOptions = new SpaceFetchOptions();
    spaceFetchOptions.withRegistrator();
    spaceFetchOptions.withProjects();
    spaceFetchOptions.withSamples();

    return spaceFetchOptions;
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Project ---------------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  public static ProjectFetchOptions fetchProjectsCompletely() {
    ProjectFetchOptions projectFetchOptions = new ProjectFetchOptions();
    projectFetchOptions.withRegistrator();
    projectFetchOptions.withLeader();
    projectFetchOptions.withSpace();
    projectFetchOptions.withExperiments();
    projectFetchOptions.withSamples();
    projectFetchOptions.withAttachments();
    projectFetchOptions.withHistory();
    projectFetchOptions.withModifier();

    return projectFetchOptions;
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Experiment / ExperimentType -------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  public static ExperimentFetchOptions fetchExperimentsCompletely() {
    ExperimentFetchOptions experimentFetchOptions = new ExperimentFetchOptions();
    experimentFetchOptions.withRegistrator();
    experimentFetchOptions.withProject();
    experimentFetchOptions.withSamples();
    experimentFetchOptions.withDataSets();
    experimentFetchOptions.withAttachments();
    experimentFetchOptions.withHistory();
    experimentFetchOptions.withModifier();
    experimentFetchOptions.withMaterialProperties();
    experimentFetchOptions.withProperties();
    experimentFetchOptions.withTags();
    experimentFetchOptions.withType();

    return experimentFetchOptions;
  }

  public static ExperimentTypeFetchOptions fetchExperimentTypesCompletely() {
    ExperimentTypeFetchOptions experimentTypeFetchOptions = new ExperimentTypeFetchOptions();
    experimentTypeFetchOptions.withPropertyAssignments();
    experimentTypeFetchOptions.withValidationPlugin();

    return experimentTypeFetchOptions;
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Sample / SampleType ---------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  public static SampleFetchOptions fetchSamplesCompletely() {
    SampleFetchOptions sampleFetchOptions = new SampleFetchOptions();
    sampleFetchOptions.withRegistrator();
    sampleFetchOptions.withSpace();
    sampleFetchOptions.withProject();
    sampleFetchOptions.withExperiment();
    sampleFetchOptions.withDataSets();
    sampleFetchOptions.withAttachments();
    sampleFetchOptions.withComponents();
    sampleFetchOptions.withContainer();
    sampleFetchOptions.withHistory();
    sampleFetchOptions.withModifier();
    sampleFetchOptions.withMaterialProperties();
    sampleFetchOptions.withProperties();
    sampleFetchOptions.withTags();
    sampleFetchOptions.withType();
    sampleFetchOptions.withParents();
    sampleFetchOptions.withChildren();

    return sampleFetchOptions;
  }

  public static SampleTypeFetchOptions fetchSampleTypesCompletely() {
    SampleTypeFetchOptions sampleTypeFetchOption = new SampleTypeFetchOptions();
    sampleTypeFetchOption.withPropertyAssignments();
    sampleTypeFetchOption.withSemanticAnnotations();
    sampleTypeFetchOption.withValidationPlugin();

    return sampleTypeFetchOption;
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- DataSet ---------------------------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  public static DataSetFetchOptions fetchDataSetsCompletely() {
    DataSetFetchOptions dataSetFetchOptions = new DataSetFetchOptions();
    dataSetFetchOptions.withRegistrator();
    dataSetFetchOptions.withExperiment();
    dataSetFetchOptions.withSample();
    dataSetFetchOptions.withDataStore();
    dataSetFetchOptions.withComponents();
    dataSetFetchOptions.withContainers();
    dataSetFetchOptions.withHistory();
    dataSetFetchOptions.withModifier();
    dataSetFetchOptions.withMaterialProperties();
    dataSetFetchOptions.withProperties();
    dataSetFetchOptions.withTags();
    dataSetFetchOptions.withType();
    dataSetFetchOptions.withLinkedData();
    dataSetFetchOptions.withParents();
    dataSetFetchOptions.withChildren();
    dataSetFetchOptions.withPhysicalData();


    return dataSetFetchOptions;
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- Vocabulary / VocabularyTerm -------------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  public static VocabularyFetchOptions fetchVocabularyCompletely() {
    VocabularyFetchOptions vocabularyFetchOptions = new VocabularyFetchOptions();
    vocabularyFetchOptions.withRegistrator();
    vocabularyFetchOptions.withTerms();

    return vocabularyFetchOptions;
  }

  public static VocabularyTermFetchOptions fetchVocabularyTermCompletely() {
    VocabularyTermFetchOptions vocabularyTermFetchOptions = new VocabularyTermFetchOptions();
    vocabularyTermFetchOptions.withRegistrator();
    vocabularyTermFetchOptions.withVocabulary();

    return vocabularyTermFetchOptions;
  }


  /* ------------------------------------------------------------------------------------ */
  /* ----- PropertyAssignmen / PropertyType --------------------------------------------- */
  /* ------------------------------------------------------------------------------------ */
  public static PropertyTypeFetchOptions fetchPropertyTypeCompletely() {
    PropertyTypeFetchOptions propertyTypeFetchOptions = new PropertyTypeFetchOptions();
    propertyTypeFetchOptions.withRegistrator();
    propertyTypeFetchOptions.withVocabulary();
    propertyTypeFetchOptions.withSemanticAnnotations();
    propertyTypeFetchOptions.withMaterialType();

    return propertyTypeFetchOptions;
  }

  public static PropertyTypeFetchOptions fetchPropertyTypeWithVocabularyAndTerms() {
    PropertyTypeFetchOptions propertyTypeFetchOptions = new PropertyTypeFetchOptions();
    propertyTypeFetchOptions.withVocabularyUsing(fetchVocabularyCompletely());

    return propertyTypeFetchOptions;
  }

  public static PropertyAssignmentFetchOptions fetchPropertyAssignmentCompletely() {
    PropertyAssignmentFetchOptions propertyAssignmentFetchOptions = new PropertyAssignmentFetchOptions();
    propertyAssignmentFetchOptions.withRegistrator();
    propertyAssignmentFetchOptions.withPropertyType();
    propertyAssignmentFetchOptions.withEntityType();
    propertyAssignmentFetchOptions.withPlugin();

    return propertyAssignmentFetchOptions;
  }

  public static PropertyAssignmentFetchOptions fetchPropertyAssignmentWithPropertyType() {
    PropertyAssignmentFetchOptions propertyAssignmentFetchOptions = new PropertyAssignmentFetchOptions();
    propertyAssignmentFetchOptions.withPropertyTypeUsing(fetchPropertyTypeCompletely());

    return propertyAssignmentFetchOptions;
  }
}
