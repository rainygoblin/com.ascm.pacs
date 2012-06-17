package com.ascm.pacs.server.listener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.dcm4che2.data.UID;
import org.dcm4che2.net.Device;
import org.dcm4che2.net.NetworkApplicationEntity;
import org.dcm4che2.net.NetworkConnection;
import org.dcm4che2.net.TransferCapability;
import org.dcm4che2.net.service.DicomService;
import org.dcm4che2.net.service.VerificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

@Component
public final class DicomListener {
	private static final Logger logger = LoggerFactory
			.getLogger(DicomListener.class);
	private static final String[] ONLY_DEF_TS = { UID.ImplicitVRLittleEndian };
	private static final String[] NON_RETIRED_LE_TS = { UID.JPEGLSLossless,
			UID.JPEGLossless, UID.JPEGLosslessNonHierarchical14,
			UID.JPEG2000LosslessOnly, UID.DeflatedExplicitVRLittleEndian,
			UID.RLELossless, UID.ExplicitVRLittleEndian,
			UID.ImplicitVRLittleEndian, UID.JPEGBaseline1, UID.JPEGExtended24,
			UID.JPEGLSLossyNearLossless, UID.JPEG2000, UID.MPEG2, };

	private String[] tsuids = NON_RETIRED_LE_TS;

	private Map<Integer, Device> workingDevices = new HashMap<Integer, Device>();


	@Resource(name = "deviceTaskExecutor")
	private TaskExecutor taskExecutor;

	@PostConstruct
	public void init() {
		logger.info("Start the Dicom Receive Service to receive dicom message.");
		start();
	}

	@PreDestroy
	public void cleanup() {
		for (Integer deviceKey : workingDevices.keySet()) {
			workingDevices.get(deviceKey).stopListening();
		}
	}

	public void start() {
		List<ServerPartition> serverPartitions = serverPartitionManager
				.loadAll();
		for (ServerPartition serverPartition : serverPartitions) {
			if (serverPartition.isEnabled()) {
				DicomListenerThread dicomListenerThread = new DicomListenerThread(
						serverPartition);
				taskExecutor.execute(dicomListenerThread);
			}
		}
	}

	private void initTransferCapability(NetworkApplicationEntity ae,
			List<DicomService> dicomServices, boolean isStgcmtEnabled) {
		int length = 0;
		for (DicomService dicomService : dicomServices) {
			length = length + dicomService.getSopClasses().length;
		}
		TransferCapability[] tc;
		if (isStgcmtEnabled) {
			tc = new TransferCapability[length + 2];
			tc[tc.length - 1] = new TransferCapability(
					UID.StorageCommitmentPushModelSOPClass, ONLY_DEF_TS,
					TransferCapability.SCP);
		} else {
			tc = new TransferCapability[length + 1];
		}

		int index = 1;
		tc[0] = new TransferCapability(UID.VerificationSOPClass, ONLY_DEF_TS,
				TransferCapability.SCP);
		for (DicomService dicomService : dicomServices) {
			int tempLength = dicomService.getSopClasses().length;
			for (int i = 0; i < tempLength; i++) {
				tc[index] = new TransferCapability(
						dicomService.getSopClasses()[i], tsuids,
						TransferCapability.SCP);
				index++;
			}
		}

		ae.setTransferCapability(tc);
	}

	private final class DicomListenerThread implements Runnable {

		private ServerPartition serverPartition;

		public DicomListenerThread(ServerPartition serverPartition) {
			this.serverPartition = serverPartition;
		}

		@Override
		public void run() {
			// initial the networkconnection
			NetworkConnection nc = new NetworkConnection();

			nc.setPort(serverPartition.getPort());
			nc.setTcpNoDelay(true);
			// nc.setTcpNoDelay(serverPartition.isTcpNoDelay());
			// nc.setAcceptTimeout(serverPartition.getAcceptTimeOut());
			// nc.setRequestTimeout(serverPartition.getRequestTimeout());
			// nc.setReleaseTimeout(serverPartition.getReleaseTimeout());
			// nc.setSocketCloseDelay(serverPartition.getSocketCloseDelay());

			NetworkApplicationEntity ae = new NetworkApplicationEntity();

			List<DicomService> dicomServices = new ArrayList<DicomService>();

			// new NewThreadExecutor(serverPartition.getName());
			ae.setPackPDV(true);
			ae.setNetworkConnection(nc);
			ae.setAssociationAcceptor(true);

			// register the verification service to device
			ae.register(new VerificationService());

			ae.register(storageSCP);
			dicomServices.add(storageSCP);

			// register the study commit scp
			// if (serverPartition.isStgcmtEnabled()) {
			// ae.register(new StgCmtSCP(serverPartition.getStgCmtPort()));
			// }

			ae.register(findSCP);
			dicomServices.add(findSCP);

			ae.register(moveSCP);
			dicomServices.add(moveSCP);

			ae.register(getSCP);
			dicomServices.add(getSCP);

			initTransferCapability(ae, dicomServices, false);
			// serverPartition.isStgcmtEnabled());

			// new Device to construct and initial
			Device device = new Device();
			// device.setAssociationReaperPeriod(serverPartition
			// .getAssociationReaperPeriod());
			device.setNetworkApplicationEntity(ae);
			device.setNetworkConnection(nc);
			// start to listening
			try {
				device.startListening(taskExecutor);
			} catch (IOException e) {
				logger.error("get error", e);
			}
		}

	}
}