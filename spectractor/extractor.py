import sys

sys.path.append('../SpectractorSim')

from spectractorsim import *
from spectractor import *
from mcmc import *
import parameters

import tqdm
from scipy.optimize import least_squares
from mpl_toolkits.axes_grid1 import make_axes_locatable
from pathos.multiprocessing import Pool


class Extractor:

    def __init__(self, filename, atmgrid_filename="", live_fit=False):
        self.my_logger = parameters.set_logger(self.__class__.__name__)
        self.filename = filename
        self.live_fit = live_fit
        self.A1 = 1.0
        self.A2 = 0.1
        self.ozone = 300.
        self.pwv = 3
        self.aerosols = 0.03
        self.reso = 10.
        self.shift = 1e-3
        self.p = np.array([self.A1, self.A2, self.ozone, self.pwv, self.aerosols, self.reso, self.shift])
        self.lambdas = None
        self.model = None
        self.model_err = None
        self.model_noconv = None
        self.labels = ["$A_1$", "$A_2$", "ozone", "PWV", "VAOD", "reso", "$\lambda_{\\mathrm{shift}}$"]
        self.bounds = ((0, 0, 0, 0, 0, 1, -20), (np.inf, 1.0, np.inf, 10, 1.0, 100, 20))
        self.title = ""
        self.spectrum, self.telescope, self.disperser, self.target = SpectractorSimInit(filename)
        self.airmass = self.spectrum.header['AIRMASS']
        self.pressure = self.spectrum.header['OUTPRESS']
        self.temperature = self.spectrum.header['OUTTEMP']
        self.use_grid = False
        if atmgrid_filename == "":
            self.atmosphere = Atmosphere(self.airmass, self.pressure, self.temperature)
        else:
            self.use_grid = True
            self.atmosphere = AtmosphereGrid(filename, atmgrid_filename)
            if parameters.VERBOSE:
                self.my_logger.info('\n\tUse atmospheric grid models from file %s. ' % atmgrid_filename)
        self.p[0] *= np.max(self.spectrum.data) / np.max(self.simulation(self.spectrum.lambdas, *self.p))
        self.get_truth()
        if 0. in self.spectrum.err:
            self.spectrum.err = np.ones_like(self.spectrum.err)
        if parameters.DEBUG:
            fig = plt.figure()
            for i in range(10):
                a = self.atmosphere.interpolate(300, i, 0.05)
                plt.plot(self.atmosphere.lambdas, a, label='pwv=%dmm' % i)
            plt.grid()
            plt.xlabel('$\lambda$ [nm]')
            plt.ylabel('Atmospheric transmission')
            plt.legend(loc='best')
            plt.show()

    def get_truth(self):
        if 'A1' in self.spectrum.header.keys():
            A1_truth = self.spectrum.header['A1']
            if 'A2' in self.spectrum.header.keys():
                A2_truth = self.spectrum.header['A2']
            if 'OZONE' in self.spectrum.header.keys():
                ozone_truth = self.spectrum.header['OZONE']
            if 'PWV' in self.spectrum.header.keys():
                pwv_truth = self.spectrum.header['PWV']
            if 'VAOD' in self.spectrum.header.keys():
                aerosols_truth = self.spectrum.header['VAOD']
            if 'RESO' in self.spectrum.header.keys():
                reso_truth = self.spectrum.header['RESO']
            self.truth = (A1_truth, A2_truth, ozone_truth, pwv_truth, aerosols_truth, reso_truth, None, None)
        else:
            self.truth = None

    def simulation(self, lambdas, A1, A2, ozone, pwv, aerosols, reso, shift=0.):
        self.title = 'Parameters: A1=%.3f, A2=%.3f, PWV=%.3f, OZ=%.3g, VAOD=%.3f, reso=%.2f, shift=%.2f' % (
        A1, A2, pwv, ozone, aerosols, reso, shift)
        self.atmosphere.simulate(ozone, pwv, aerosols)
        simulation = SpectrumSimulation(self.spectrum, self.atmosphere, self.telescope, self.disperser)
        simulation.simulate(lambdas - shift)
        self.model_noconv = A1 * np.copy(simulation.data)
        sim_conv = fftconvolve_gaussian(simulation.data, reso)
        err_conv = np.sqrt(fftconvolve_gaussian(simulation.err ** 2, reso))
        sim_conv = interp1d(lambdas, sim_conv, kind="linear", bounds_error=False, fill_value=(0, 0))
        err_conv = interp1d(lambdas, err_conv, kind="linear", bounds_error=False, fill_value=(0, 0))
        self.lambdas = lambdas
        self.model = lambda x: A1 * sim_conv(x) + A1 * A2 * sim_conv(x / 2)
        self.model_err = lambda x: np.sqrt((A1 * err_conv(x)) ** 2 + (0.5 * A1 * A2 * err_conv(x / 2)) ** 2)
        if self.live_fit: self.plot_fit()
        return self.model(lambdas), self.model_err(lambdas)

    def chisq(self, p):
        model, err = self.simulation(self.spectrum.lambdas, *p)
        chisq = np.sum((model - self.spectrum.data) ** 2 / (err ** 2 + self.spectrum.err ** 2))
        # chisq /= self.spectrum.data.size
        # print '\tReduced chisq =',chisq/self.spectrum.data.size
        return chisq

    def minimizer(self):
        res = least_squares(self.chisq, x0=self.p, bounds=self.bounds, xtol=1e-6, ftol=1e-5, method='trf', verbose=0,
                            x_scale=(1, 1000, 10000, 10000, 10, 1000, 1000, 1000), loss='soft_l1')
        # diff_step=(0.1,0.1,0.5,1,0.5,0.2),
        self.popt = res.x
        print res
        print res.x

    def plot_spectrum_comparison_simple(self, ax, title='', extent=None, size=0.4):
        self.spectrum.plot_spectrum_simple(ax)
        sub = np.where((self.lambdas > parameters.LAMBDA_MIN) & (self.lambdas < parameters.LAMBDA_MAX))
        if extent != None:
            sub = np.where((self.lambdas > extent[0]) & (self.lambdas < extent[1]))
        p0 = ax.plot(self.lambdas, self.model(self.lambdas), label='model')
        ax.fill_between(self.lambdas, self.model(self.lambdas) - self.model_err(self.lambdas),
                        self.model(self.lambdas) + self.model_err(self.lambdas), alpha=0.3, color=p0[0].get_color())
        ax.plot(self.lambdas, self.model_noconv, label='before conv')
        if title != '': ax.set_title(title, fontsize=10)
        ax.legend()
        divider = make_axes_locatable(ax)
        ax2 = divider.append_axes("bottom", size=size, pad=0)
        ax.figure.add_axes(ax2)
        residuals = (self.spectrum.data - self.model(self.lambdas)) / self.model(self.lambdas)
        residuals_err = self.spectrum.err / self.model(self.lambdas)
        ax2.errorbar(self.lambdas, residuals, yerr=residuals_err, fmt='ro', markersize=2)
        ax2.axhline(0, color=p0[0].get_color())
        ax2.grid(True)
        residuals_model = self.model_err(self.lambdas) / self.model(self.lambdas)
        ax2.fill_between(self.lambdas, -residuals_model, residuals_model, alpha=0.3, color=p0[0].get_color())
        std = np.std(residuals[sub])
        ax2.set_ylim([-2. * std, 2. * std])
        ax2.set_xlabel(ax.get_xlabel())
        ax2.set_ylabel('(data-fit)/fit')
        ax2.set_xlim((self.lambdas[sub][0], self.lambdas[sub][-1]))
        ax.set_xlim((self.lambdas[sub][0], self.lambdas[sub][-1]))
        ax.set_ylim((0.9 * np.min(self.spectrum.data[sub]), 1.1 * np.max(self.spectrum.data[sub])))
        ax.set_xticks(ax2.get_xticks()[1:-1])

    def plot_fit(self):
        fig = plt.figure(figsize=(12, 6))
        ax1 = plt.subplot(222)
        ax2 = plt.subplot(224)
        ax3 = plt.subplot(121)
        # main plot
        self.plot_spectrum_comparison_simple(ax3, title=self.title, size=0.8)
        # zoom O2
        self.plot_spectrum_comparison_simple(ax2, extent=[730, 800], title='Zoom $O_2$', size=0.8)
        # zoom H2O
        self.plot_spectrum_comparison_simple(ax1, extent=[870, 1000], title='Zoom $H_2 O$', size=0.8)
        fig.tight_layout()
        if self.live_fit:
            plt.draw()
            plt.pause(1e-8)
            plt.close()
        else:
            plt.show()


class Extractor_MCMC(Extractor):

    def __init__(self, filename, covfile, nchains=1, nsteps=1000, burnin=100, nbins=10, exploration_time=100,
                 atmgrid_filename="", live_fit=False):
        Extractor.__init__(self, filename, atmgrid_filename=atmgrid_filename, live_fit=live_fit)
        # self.ndim = len(self.p)
        # self.nwalkers = 4*self.ndim
        self.nchains = nchains
        self.nsteps = nsteps
        self.covfile = covfile
        self.nbins = nbins
        self.burnin = burnin
        self.exploration_time = exploration_time
        self.chains = Chains(filename, covfile, nchains, nsteps, burnin, nbins, truth=self.truth)
        self.covfile = filename.replace('spectrum.fits', 'cov.txt')
        self.results = []
        self.results_err = []
        for i in range(self.chains.dim):
            self.results.append(ParameterList(self.chains.labels[i], self.chains.axis_names[i]))
            self.results_err.append([])

    def lnprior(self, p):
        in_bounds = True
        for npar, par in enumerate(p):
            if par < self.bounds[0][npar] or par > self.bounds[1][npar]:
                in_bounds = False
                break
        if in_bounds:
            return 0.0
        else:
            return -1e20

    def lnlike(self, p):
        return -0.5 * self.chisq(p)

    def lnprob(self, p):
        lp = self.lnprior(p)
        if not np.isfinite(lp):
            return -1e20
        return lp + self.lnlike(p)

    def prior(self, p):
        in_bounds = True
        for npar, par in enumerate(p):
            if par < self.bounds[0][npar] or par > self.bounds[1][npar]:
                in_bounds = False
                break
        if in_bounds:
            return 1.0
        else:
            return 0.

    def likelihood(self, p):
        return np.exp(-0.5 * self.chisq(p))

    def posterior(self, p):
        prior = self.prior(p)
        if np.isclose(prior, 0.):
            return 0.
        else:
            return prior * self.likelihood(p)

    def mcmc_emcee(self):
        pos = np.array([self.p + 0.1 * self.p * np.random.randn(self.ndim) for i in range(self.nwalkers)])
        # Set up the backend
        # Don't forget to clear it in case the file already exists
        filename = "tutorial.h5"
        # backend = emcee.backends.HDFBackend(filename)
        # backend.reset(self.nwalkers, self.ndim)

        # with Pool() as pool:
        self.sampler = emcee.EnsembleSampler(self.nwalkers, self.ndim, self.lnprob, args=())
        nsamples = 6000
        self.sampler.run_mcmc(pos, nsamples)
        # tau = sampler.get_autocorr_time()
        burnin = nsamples / 2
        thin = nsamples / 4
        # self.samples = sampler.get_chain(discard=burnin, flat=True, thin=thin)
        self.samples = self.sampler.chain[:, burnin:, :].reshape((-1, self.ndim))
        # log_prob_samples = sampler.get_log_prob(discard=burnin, flat=True, thin=thin)
        # log_prior_samples = sampler.get_blobs(discard=burnin, flat=True, thin=thin)

        print("burn-in: {0}".format(burnin))
        print("thin: {0}".format(thin))
        # print("flat chain shape: {0}".format(samples.shape))
        # print("flat log prob shape: {0}".format(log_prob_samples.shape))
        # print("flat log prior shape: {0}".format(log_prior_samples.shape))

        fig = corner.corner(self.samples, labels=self.labels, truths=self.truth, quantiles=[0.16, 0.5, 0.84],
                            show_titles=True)
        plt.show()
        fig.savefig("triangle.png")

    def mcmc(self, chain):
        np.random.seed(chain.nchain)  # very important othewise parallel processes have same random generator
        vec1 = chain.start_vec
        prior1 = 1
        # initialisation of the chain
        if chain.start_key == -1:
            prior1 = 1e-10
            while prior1 < 1e-9:
                vec1 = chain.draw_vector(chain.start_vec)
                prior1 = self.prior(vec1)
        else:
            chain.start_index += 1
        sim = self.simulation(self.spectrum.lambdas, *vec1)
        if np.max(sim) > 0: vec1[0] *= np.max(self.spectrum.data) / np.max(sim)
        if parameters.DEBUG: print "First vector : ", vec1
        chisq1 = self.chisq(vec1)
        L1 = np.exp(-0.5 * chisq1 + 0.5 * self.spectrum.lambdas.size)
        # MCMC exploration
        keys = range(chain.start_index, chain.nsteps)
        new_keys = []
        # import time
        for i in tqdm.tqdm(keys, desc='Processing chain %i:' % chain.nchain, position=chain.nchain):
            # start = time.time()
            if parameters.DEBUG:
                print 'Step : %d (start=%d, stop=%d, remaining nsteps=%d)' % (
                i, chain.start_index, chain.start_index + chain.nsteps - 1, chain.nsteps + chain.start_index - i)
            vec2 = []
            prior2 = 1
            # print 'init',time.time()-start
            # start = time.time()
            vec2 = chain.draw_vector(vec1)
            prior2 = self.prior(vec2)
            # print 'prior',time.time()-start
            # start = time.time()

            if prior2 > 1e-10:
                chisq2 = self.chisq(vec2)
            else:
                chisq2 = 1e20
            L2 = np.exp(-0.5 * chisq2)  # +0.5*self.spectrum.lambdas.size)
            # print 'chisq',time.time()-start
            # start = time.time()
            if parameters.DEBUG:
                print "Sample chisq : %.2f      Prior : %.2f" % (chisq2, prior2)
                print "Sample vector : ", vec2
            r = np.random.uniform(0, 1)
            # if L1>0 and L2/L1 > r :
            if np.exp(-0.5 * (chisq2 - chisq1)) > r:
                dictline = chain.make_dictline(i, chisq2, vec2)
                vec1 = vec2
                L1 = L2
                chisq1 = chisq2
            else:
                dictline = chain.make_dictline(i, chisq1, vec1)
            new_key = chain.newrow(dictline, key=i + chain.nchain * chain.nsteps)
            # print 'newrow',time.time()-start
            # start = time.time()
            # chain.append2filelastkey(self.chains.chains_filename)
            if i > self.exploration_time:
                chain.update_proposal_cov(vec1)
            # print 'proposal',time.time()-start
            # start = time.time()
        chain.append2file(self.chains.chains_filename)

    def run_mcmc(self):
        complete = self.chains.check_completness()
        if not complete:
            for i in range(self.nchains):
                self.chains.chains.append(
                    Chain(self.chains.chains_filename, self.covfile, nchain=i, nsteps=self.nsteps))
            pool = Pool(processes=self.nchains)
            try:
                # Without the .get(9999), you can't interrupt this with Ctrl+C.
                pool.map_async(self.mcmc, self.chains.chains).get(999999)
                pool.close()
                pool.join()
                # to skip lines after the progress bars
                print '\n' * self.nchains
            except KeyboardInterrupt:
                pool.terminate()
        self.likelihood = self.chains.chains_to_likelihood()
        self.likelihood.stats(self.covfile)
        # [self.results[i].append(self.likelihood.pdfs[i].mean) for i in range(self.chains.dim)]
        # self.p = [self.likelihood.pdfs[i].mean for i in range(self.chains.dim)]
        self.p = self.chains.best_row_params
        self.simulation(self.spectrum.lambdas, *self.p)
        # [self.results_err[i].append([self.likelihood.pdfs[i].error_high,self.likelihood.pdfs[i].error_low]) for i in range(self.chains.dim)]
        # if(self.plot):
        self.likelihood.triangle_plots()
        self.plot_fit()
        # if convergence_test :
        self.chains.convergence_tests()
        return self.likelihood


if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-d", "--debug", dest="debug", action="store_true",
                      help="Enter debug mode (more verbose and plots).", default=False)
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                      help="Enter verbose (print more stuff).", default=False)
    parser.add_option("-o", "--output_directory", dest="output_directory", default="outputs/",
                      help="Write results in given output directory (default: ./outputs/).")
    (opts, args) = parser.parse_args()

    parameters.VERBOSE = False
    filename = 'outputs/data_30may17/sim_20170530_134_spectrum.fits'
    atmgrid_filename = filename.replace('sim', 'reduc').replace('spectrum', 'atmsim')
    filename = 'outputs/data_30may17/reduc_20170530_134_spectrum.fits'

    # m = Extractor(filename,atmgrid_filename)
    # m.minimizer(live_fit=True)
    covfile = 'covariances/proposal.txt'
    m = Extractor_MCMC(filename, covfile, nchains=4, nsteps=10000, burnin=2000, nbins=10, exploration_time=500,
                       atmgrid_filename=atmgrid_filename, live_fit=False)
    m.run_mcmc()
