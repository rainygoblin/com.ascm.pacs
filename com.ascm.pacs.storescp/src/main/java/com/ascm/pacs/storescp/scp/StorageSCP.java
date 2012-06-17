package com.ascm.pacs.storescp.scp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Resource;

import org.apache.commons.io.FilenameUtils;
import org.dcm4che2.data.BasicDicomObject;
import org.dcm4che2.data.DicomObject;
import org.dcm4che2.data.Tag;
import org.dcm4che2.net.Association;
import org.dcm4che2.net.CommandUtils;
import org.dcm4che2.net.DicomServiceException;
import org.dcm4che2.net.PDVInputStream;
import org.dcm4che2.net.Status;
import org.dcm4che2.net.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import com.ascm.pacs.common.bo.ServerFilesystemInfo;
import com.ascm.pacs.common.bo.StudyStorageLocation;
import com.ascm.pacs.common.command.MoveFileCommand;
import com.ascm.pacs.common.command.UpdateWorkQueueCommand;
import com.ascm.pacs.common.enumeration.DuplicateSopPolicyEnum;
import com.ascm.pacs.common.exception.NoWritableFilesystemException;
import com.ascm.pacs.common.exception.StudyIsNearlineException;
import com.ascm.pacs.common.exception.StudyNotFoundException;
import com.ascm.pacs.common.exception.TransferSyntaxUnsupportException;
import com.ascm.pacs.common.model.ServerPartition;
import com.ascm.pacs.common.model.Study;
import com.ascm.pacs.common.model.StudyStorage;
import com.ascm.pacs.common.processor.ServerCommandProcessor;
import com.ascm.pacs.common.service.IDeviceManager;
import com.ascm.pacs.common.service.IFilesystemManager;
import com.ascm.pacs.common.service.IServerPartitionManager;
import com.ascm.pacs.common.service.IStudyManager;
import com.ascm.pacs.common.service.IWorkQueueManager;
import com.ascm.pacs.common.util.DicomPersistenceUtil;
import com.ascm.pacs.common.util.IncludeFilteredDicomObject;
import com.ascm.pacs.common.util.IntegerOrNamedTag;
import com.ascm.pacs.dcm.stream.DicomStreamWriteCallback;
import com.ascm.pacs.dcm.util.SopClasses;
import com.ascm.pacs.hornetq.sender.StudyProcessMessageSender;

@Component
public final class StorageSCP extends StorageService {
	private static final Logger logger = LoggerFactory
			.getLogger(StorageSCP.class);
	private int rspdelay = 0;

	@Resource(name = "scpTaskExecutor")
	private TaskExecutor taskExecutor;

	@Autowired
	private IServerPartitionManager serverPartitionManager;

	@Autowired
	private IDeviceManager deviceManager;

	@Autowired
	private IStudyManager studyManager;

	@Autowired
	private IFilesystemManager filesystemManager;

	@Autowired
	private IWorkQueueManager workQueueManager;
	
	@Autowired
	private StudyProcessMessageSender studyProcessMessageSender;
	/**
	 * we persist a filtered copy of the DICOM header in the jobqueue entry in
	 * the DB this list of DICOM attribute tags determines which attributes are
	 * retained in the persisted header
	 */
	private Set<IntegerOrNamedTag> headerAttributeTags = new HashSet<IntegerOrNamedTag>();

	public StorageSCP() {
		super(SopClasses.sopClasses);

		headerAttributeTags.add(new IntegerOrNamedTag(Tag.StudyInstanceUID));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.SeriesInstanceUID));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.SOPInstanceUID));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.SOPClassUID));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.PatientID));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.IssuerOfPatientID));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.AccessionNumber));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.PatientName));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.PatientBirthDate));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.PatientBirthTime));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.PatientSex));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.StudyDate));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.StudyTime));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.StudyDescription));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.PatientWeight));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.PatientSize));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.PatientAge));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.InstitutionName));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.Modality));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.SeriesDescription));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.SeriesNumber));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.BodyPartExamined));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.Manufacturer));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.PerformingPhysicianName));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.OperatorsName));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.PatientPosition));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.InstanceNumber));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.NumberOfFrames));
		headerAttributeTags.add(new IntegerOrNamedTag(Tag.TransferSyntaxUID));
	}

	/**
	 * Overwrite {@link StorageService#cstore} to send delayed C-STORE RSP by
	 * separate Thread, so reading of following received C-STORE RQs from the
	 * open association is not blocked.
	 */
	@Override
	public void cstore(final Association as, final int pcid, DicomObject rq,
			PDVInputStream dataStream, String tsuid)
			throws DicomServiceException, IOException {
		final DicomObject rsp = CommandUtils.mkRSP(rq, CommandUtils.SUCCESS);

		onCStoreRQ(as, pcid, rq, dataStream, tsuid, rsp);

		if (rspdelay > 0) {
			// final int dimseRspDelay=currentPartition.getDimseRspDelay();
			taskExecutor.execute(new Runnable() {
				public void run() {
					try {
						logger.info("Time {} to delay.", rspdelay);
						Thread.sleep(rspdelay);
						as.writeDimseRSP(pcid, rsp);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
		} else {
			as.writeDimseRSP(pcid, rsp);
		}

		onCStoreRSP(as, pcid, rq, dataStream, tsuid, rsp);
	}

	protected void onCStoreRQ(Association as, int pcid, DicomObject rq,
			PDVInputStream dataStream, String tsuid, DicomObject rsp)
			throws IOException, DicomServiceException {

		// 1. get the currentPartition
		ServerPartition currentPartition = serverPartitionManager
				.findByAETitle(as.getCalledAET());

		// 2. get the remoteDevice. if not exist,then save the new device.
		com.ascm.pacs.common.model.Device remoteDevice = deviceManager
				.lookupDevice(currentPartition, as.getCallingAET(), as
						.getSocket().getRemoteSocketAddress().toString(), as
						.getSocket().getPort());
		if (remoteDevice == null) {
			logger.error("The calling remoteDevice should not be null!");
			throw new DicomServiceException(rq, Status.ProcessingFailure,
					"The calling remoteDevice should not be null!");
		}

		// 3. get the stored file system.
		ServerFilesystemInfo serverFilesystemInfo = filesystemManager
				.selectFilesystem();
		if (serverFilesystemInfo == null) {
			logger.error("Can not get the stored file system.");
			throw new DicomServiceException(rq, Status.ProcessingFailure,
					"Can not get the stored file system!");
		}

		// 4. store the dicom object to the temp dir
		BasicDicomObject fileMetaInfoObject = null;
		String cuid = rq.getString(Tag.AffectedSOPClassUID);
		String iuid = rq.getString(Tag.AffectedSOPInstanceUID);
		fileMetaInfoObject = new BasicDicomObject();
		fileMetaInfoObject.initFileMetaInformation(cuid, iuid, tsuid);

		DicomStreamWriteCallback dicomStreamWriteCallback = new DicomStreamWriteCallback();
		dicomStreamWriteCallback.setFileMetaInfoObject(fileMetaInfoObject);

		String tempFileName = FilenameUtils
				.concat(serverFilesystemInfo.getTempDir(), UUID.randomUUID()
						.toString());
		dicomStreamWriteCallback.doWrite(dataStream, new FileOutputStream(
				tempFileName));

		DicomObject dicomHeader = dicomStreamWriteCallback.getDicomHeader();
		if (dicomHeader == null) {
			throw new DicomServiceException(rq, Status.ProcessingFailure,
					"Can not get the Dicom header");
		}

		// 5. to process the incoming SOP
		String transferSyntaxUID = dicomHeader.getString(Tag.TransferSyntaxUID);
		String studyInstanceUid = dicomHeader.getString(Tag.StudyInstanceUID);
		String seriesInstanceUid = dicomHeader.getString(Tag.SeriesInstanceUID);
		String sopInstanceUid = dicomHeader.getString(Tag.SOPInstanceUID);
		String studyDate = dicomHeader.getString(Tag.StudyDate);

		ServerCommandProcessor commandProcessor = new ServerCommandProcessor(
				String.format("Processing Sop Instance %s", sopInstanceUid));

		IncludeFilteredDicomObject aFilteredHeader = new IncludeFilteredDicomObject(
				dicomHeader, headerAttributeTags);
		byte[] dicomHeaderByte = DicomPersistenceUtil.dicomToByteArray(
				fileMetaInfoObject, aFilteredHeader);

		String failureMessage;
		StudyStorageLocation studyLocation = null;
		try {
			studyLocation = getWritableOnlineStorage(currentPartition,
					studyInstanceUid, studyDate, transferSyntaxUID);

			if (studyLocation == null) {
				logger.error("Can not get the study location.");
				throw new DicomServiceException(rq, Status.ProcessingFailure,
						"Can not get the study location!");
			}

			String finalDest = studyLocation.getSopInstancePath(
					seriesInstanceUid, sopInstanceUid);

			if (hasUnprocessedCopy(studyLocation.getStudyStorage(),
					seriesInstanceUid, sopInstanceUid)) {
				failureMessage = String
						.format("Another copy of the SOP Instance was received but has not been processed: %s",
								sopInstanceUid);
				logger.error(failureMessage);
				throw new DicomServiceException(rq,
						Status.DuplicateSOPinstance, failureMessage);
			}
			File finalDestFile = new File(finalDest);
			if (finalDestFile.exists()) {
				String reconcileSopInstancePath = FilenameUtils.concat(
						serverFilesystemInfo.getReconcileStorageFolder(),
						studyLocation.getReconcileSopInstancePath(
								remoteDevice.getAeTitle(), studyInstanceUid,
								seriesInstanceUid, sopInstanceUid));
				handleDuplicate(currentPartition, sopInstanceUid,
						seriesInstanceUid, studyInstanceUid, tempFileName,
						reconcileSopInstancePath, remoteDevice, studyLocation,
						commandProcessor);

			} else {
				handleNonDuplicate(seriesInstanceUid, sopInstanceUid,
						studyLocation, tempFileName, finalDest, remoteDevice,
						commandProcessor, dicomHeaderByte);
			}

			if (!commandProcessor.execute()) {
				failureMessage = String
						.format("Failure processing message: %s. Sending failure status.",
								commandProcessor.getFailureReason());
				logger.error(failureMessage);
				throw new DicomServiceException(rq, Status.ProcessingFailure,
						failureMessage);
			}
		} catch (NoWritableFilesystemException e) {

			failureMessage = String
					.format("Unable to process image, no writable filesystem found for Study UID %s.",
							sopInstanceUid);
			logger.error(failureMessage);
			throw new DicomServiceException(rq, Status.ProcessingFailure,
					failureMessage);
		} catch (StudyIsNearlineException e) {
			failureMessage = e.isRestoreRequested() ? String.format(
					"%s. Restore has been requested.", e.getMessage()) : e
					.getMessage();

			commandProcessor.rollback();
			logger.error(failureMessage);
			throw new DicomServiceException(rq, Status.ProcessingFailure,
					failureMessage);
		} catch (Throwable e) {
			failureMessage = String.format("%s.  Rolling back operation.",
					e.getMessage());
			commandProcessor.rollback();
			logger.error(failureMessage);
			throw new DicomServiceException(rq, Status.ProcessingFailure,
					failureMessage);
		}

		// delete the temp dicom file
		if (new File(tempFileName).exists()) {
			new File(tempFileName).delete();
		}
		commandProcessor = null;
		studyProcessMessageSender.send("e6df31aa-f621-481e-bbba-cf72f2c1e247");
		super.onCStoreRQ(as, pcid, rq, dataStream, tsuid, rsp);
	}

	private void handleNonDuplicate(String seriesInstanceUid,
			String sopInstanceUid, StudyStorageLocation studyLocation,
			String tempFileName, String finalDest,
			com.ascm.pacs.common.model.Device remoteDevice,
			ServerCommandProcessor commandProcessor, byte[] dicomHeaderByte)
			throws IOException {
		commandProcessor
				.addCommand(new MoveFileCommand(tempFileName, finalDest));

		commandProcessor.addCommand(new UpdateWorkQueueCommand(remoteDevice,
				seriesInstanceUid, studyLocation, sopInstanceUid,
				dicomHeaderByte));

	}

	private void handleDuplicate(ServerPartition currentServerPartition,
			String sopInstanceUid, String seriesInstanceUid,
			String studyInstanceUid, String tempFileName,
			String reconcileSopInstancePath,
			com.ascm.pacs.common.model.Device remoteDevice,
			StudyStorageLocation studyLocation,
			ServerCommandProcessor commandProcessor)
			throws DicomServiceException {
		Study study = studyManager.findStudy(studyInstanceUid,
				currentServerPartition);

		if (study != null) {
			logger.info(String
					.format("Received duplicate SOP %s (A#:%s StudyUid:%s  Patient: %s  ID:%s)",
							sopInstanceUid, study.getAccessionNumber(),
							study.getStudyInstanceUid(),
							study.getPatientName(), study.getPatientId()));
		} else {
			logger.info(String
					.format("Received duplicate SOP %s (StudyUid:%s). Existing files haven't been processed.",
							sopInstanceUid, studyInstanceUid));
		}
		String failureMessage;

		if (currentServerPartition.getDuplicateSopPolicyEnum() == DuplicateSopPolicyEnum.SendSuccess) {
			logger.info(String
					.format("Duplicate SOP Instance received, sending success response %s",
							sopInstanceUid));
			return;
		}
		if (currentServerPartition.getDuplicateSopPolicyEnum() == DuplicateSopPolicyEnum.RejectDuplicates) {
			failureMessage = String.format(
					"Duplicate SOP Instance received, rejecting %s",
					sopInstanceUid);
			logger.error(failureMessage);
			throw new DicomServiceException(null, Status.DuplicateSOPinstance,
					failureMessage);
		}
		if (currentServerPartition.getDuplicateSopPolicyEnum() == DuplicateSopPolicyEnum.CompareDuplicates) {
			commandProcessor.addCommand(new MoveFileCommand(tempFileName,
					reconcileSopInstancePath));
			commandProcessor.addCommand(new UpdateWorkQueueCommand(
					remoteDevice, seriesInstanceUid, studyLocation,
					sopInstanceUid, true, "dup", remoteDevice.getAeTitle()));
		} else {
			failureMessage = String
					.format("Duplicate SOP Instance received. Unsupported duplicate policy %s.",
							currentServerPartition.getDuplicateSopPolicyEnum());
			logger.error(failureMessage);
			throw new DicomServiceException(null, Status.DuplicateSOPinstance,
					failureMessage);
		}

	}

	private boolean hasUnprocessedCopy(StudyStorage studyStorage,
			String seriesInstanceUid, String sopInstanceUid) {
		if (workQueueManager.workQueueUidExists(studyStorage,
				seriesInstanceUid, sopInstanceUid))
			return true;
		// return studyIntegrityQueueManager.studyIntegrityUidExists(
		// studyStorageId, seriesInstanceUid, sopInstanceUid);
		return false;
	}

	private StudyStorageLocation getWritableOnlineStorage(
			ServerPartition serverPartition, String studyInstanceUid,
			String studyDate, String transferSyntaxUID)
			throws NoWritableFilesystemException, StudyIsNearlineException,
			StudyNotFoundException, TransferSyntaxUnsupportException {
		StudyStorageLocation studyLocation = filesystemManager
				.getOrCreateWritableStudyStorageLocation(studyInstanceUid,
						studyDate, transferSyntaxUID, serverPartition);

		return studyLocation;
	}

}