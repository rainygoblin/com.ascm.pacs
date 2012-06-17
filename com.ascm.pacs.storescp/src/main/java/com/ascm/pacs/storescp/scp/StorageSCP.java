package com.ascm.pacs.storescp.scp;

import java.io.IOException;

import javax.annotation.Resource;

import org.dcm4che2.data.DicomObject;
import org.dcm4che2.data.UID;
import org.dcm4che2.net.Association;
import org.dcm4che2.net.CommandUtils;
import org.dcm4che2.net.DicomServiceException;
import org.dcm4che2.net.PDVInputStream;
import org.dcm4che2.net.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

@Component
public final class StorageSCP extends StorageService {
	private static final Logger logger = LoggerFactory
			.getLogger(StorageSCP.class);
	private int rspdelay = 0;
	public static final String[] sopClasses = {
		UID.BasicStudyContentNotificationSOPClassRetired,
		UID.StoredPrintStorageSOPClassRetired,
		UID.HardcopyGrayscaleImageStorageSOPClassRetired,
		UID.HardcopyColorImageStorageSOPClassRetired,
		UID.ComputedRadiographyImageStorage,
		UID.DigitalXRayImageStorageForPresentation,
		UID.DigitalXRayImageStorageForProcessing,
		UID.DigitalMammographyXRayImageStorageForPresentation,
		UID.DigitalMammographyXRayImageStorageForProcessing,
		UID.DigitalIntraoralXRayImageStorageForPresentation,
		UID.DigitalIntraoralXRayImageStorageForProcessing,
		UID.StandaloneModalityLUTStorageRetired,
		UID.EncapsulatedPDFStorage, UID.StandaloneVOILUTStorageRetired,
		UID.GrayscaleSoftcopyPresentationStateStorageSOPClass,
		UID.ColorSoftcopyPresentationStateStorageSOPClass,
		UID.PseudoColorSoftcopyPresentationStateStorageSOPClass,
		UID.BlendingSoftcopyPresentationStateStorageSOPClass,
		UID.XRayAngiographicImageStorage, UID.EnhancedXAImageStorage,
		UID.XRayRadiofluoroscopicImageStorage, UID.EnhancedXRFImageStorage,
		UID.XRayAngiographicBiPlaneImageStorageRetired,
		UID.PositronEmissionTomographyImageStorage,
		UID.StandalonePETCurveStorageRetired, UID.CTImageStorage,
		UID.EnhancedCTImageStorage, UID.NuclearMedicineImageStorage,
		UID.UltrasoundMultiframeImageStorageRetired,
		UID.UltrasoundMultiframeImageStorage, UID.MRImageStorage,
		UID.EnhancedMRImageStorage, UID.MRSpectroscopyStorage,
		UID.RTImageStorage, UID.RTDoseStorage, UID.RTStructureSetStorage,
		UID.RTBeamsTreatmentRecordStorage, UID.RTPlanStorage,
		UID.RTBrachyTreatmentRecordStorage,
		UID.RTTreatmentSummaryRecordStorage,
		UID.NuclearMedicineImageStorageRetired,
		UID.UltrasoundImageStorageRetired, UID.UltrasoundImageStorage,
		UID.RawDataStorage, UID.SpatialRegistrationStorage,
		UID.SpatialFiducialsStorage, UID.RealWorldValueMappingStorage,
		UID.SecondaryCaptureImageStorage,
		UID.MultiframeSingleBitSecondaryCaptureImageStorage,
		UID.MultiframeGrayscaleByteSecondaryCaptureImageStorage,
		UID.MultiframeGrayscaleWordSecondaryCaptureImageStorage,
		UID.MultiframeTrueColorSecondaryCaptureImageStorage,
		UID.VLImageStorageTrialRetired, UID.VLEndoscopicImageStorage,
		UID.VideoEndoscopicImageStorage, UID.VLMicroscopicImageStorage,
		UID.VideoMicroscopicImageStorage,
		UID.VLSlideCoordinatesMicroscopicImageStorage,
		UID.VLPhotographicImageStorage, UID.VideoPhotographicImageStorage,
		UID.OphthalmicPhotography8BitImageStorage,
		UID.OphthalmicPhotography16BitImageStorage,
		UID.StereometricRelationshipStorage,
		UID.VLMultiframeImageStorageTrialRetired,
		UID.StandaloneOverlayStorageRetired, UID.BasicTextSRStorage,
		UID.EnhancedSRStorage, UID.ComprehensiveSRStorage,
		UID.ProcedureLogStorage, UID.MammographyCADSRStorage,
		UID.KeyObjectSelectionDocumentStorage, UID.ChestCADSRStorage,
		UID.XRayRadiationDoseSRStorage, UID.EncapsulatedPDFStorage,
		UID.EncapsulatedCDAStorage, UID.StandaloneCurveStorageRetired,
		UID._12leadECGWaveformStorage, UID.GeneralECGWaveformStorage,
		UID.AmbulatoryECGWaveformStorage, UID.HemodynamicWaveformStorage,
		UID.CardiacElectrophysiologyWaveformStorage,
		UID.BasicVoiceAudioWaveformStorage, UID.HangingProtocolStorage,
		UID.SiemensCSANonImageStorage,
		UID.Dcm4cheAttributesModificationNotificationSOPClass };

	@Resource(name = "scpTaskExecutor")
	private TaskExecutor taskExecutor;

	
	public StorageSCP() {
		super(sopClasses);

		
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

		
	}

}